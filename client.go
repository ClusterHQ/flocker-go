package flocker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// From https://github.com/ClusterHQ/flocker-docker-plugin/blob/master/flockerdockerplugin/adapter.py#L18
const defaultVolumeSize = json.Number("107374182400")

var (
	// A volume can take a long time to be available, if we don't want
	// Kubernetes to wait forever we need to stop trying after some time, that
	// time is defined here
	timeoutWaitingForVolume = 2 * time.Minute
	tickerWaitingForVolume  = 5 * time.Second

	errStateNotFound         = errors.New("State not found by Dataset ID")
	errConfigurationNotFound = errors.New("Configuration not found by Name")

	errFlockerControlServiceHost = errors.New("The volume config must have a key CONTROL_SERVICE_HOST defined in the OtherAttributes field")
	errFlockerControlServicePort = errors.New("The volume config must have a key CONTROL_SERVICE_PORT defined in the OtherAttributes field")

	errVolumeAlreadyExists = errors.New("The volume already exists")
	errVolumeDoesNotExist  = errors.New("The volume does not exist")

	errUpdatingDataset = errors.New("It was impossible to update the dataset")
)

type flockerClient struct {
	*http.Client

	schema  string
	host    string
	port    int
	version string

	maximumSize json.Number
}

// NewClient creates a wrapper over http.Client to communicate with the flocker control service.
func NewClient(host string, port int, caCertPath, keyPath, certPath string) (*flockerClient, error) {
	client, err := newTLSClient(caCertPath, keyPath, certPath)
	if err != nil {
		return nil, err
	}

	return &flockerClient{
		Client:      client,
		schema:      "https",
		host:        host,
		port:        port,
		version:     "v1",
		maximumSize: defaultVolumeSize,
	}, nil
}

/*
request do a request using the http.Client embedded to the control service
and returns the response or an error in case it happens.

Note: you will need to deal with the response body call to Close if you
don't want to deal with problems later.
*/
func (c flockerClient) request(method, url string, payload interface{}) (*http.Response, error) {
	var (
		b   []byte
		err error
	)

	if method == "POST" { // Just allow payload on POST
		b, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// REMEMBER TO CLOSE THE BODY IN THE OUTSIDE FUNCTION
	return c.Do(req)
}

// post performs a post request with the indicated payload
func (c flockerClient) post(url string, payload interface{}) (*http.Response, error) {
	return c.request("POST", url, payload)
}

// get performs a get request
func (c flockerClient) get(url string) (*http.Response, error) {
	return c.request("GET", url, nil)
}

// getURL returns a full URI to the control service
func (c flockerClient) getURL(path string) string {
	return fmt.Sprintf("%s://%s:%d/%s/%s", c.schema, c.host, c.port, c.version, path)
}

type configurationPayload struct {
	Primary     string          `json:"primary"`
	DatasetID   string          `json:"dataset_id,omitempty"`
	MaximumSize json.Number     `json:"maximum_size,omitempty"`
	Metadata    metadataPayload `json:"metadata,omitempty"`
}

type metadataPayload struct {
	Name string `json:"name,omitempty"`
}

type datasetState struct {
	Path        string      `json:"path"`
	DatasetID   string      `json:"dataset_id"`
	Primary     string      `json:"primary,omitempty"`
	MaximumSize json.Number `json:"maximum_size,omitempty"`
}

type datasetStatePayload struct {
	*datasetState
}

type nodeStatePayload struct {
	UUID string `json:"uuid"`
	Host string `json:"host"`
}

// findIDInConfigurationsPayload returns the datasetID if it was found in the
// configurations payload, otherwise it will return an error.
func (c flockerClient) findIDInConfigurationsPayload(body io.ReadCloser, name string) (datasetID string, err error) {
	var configurations []configurationPayload
	if err = json.NewDecoder(body).Decode(&configurations); err == nil {
		for _, r := range configurations {
			if r.Metadata.Name == name {
				return r.DatasetID, nil
			}
		}
		return "", errConfigurationNotFound
	}
	return "", err
}

// findPrimaryUUID returns the UUID of the primary Flocker Control Service for
// the given host.
func (c flockerClient) findPrimaryUUID() (uuid string, err error) {
	resp, err := c.get(c.getURL("state/nodes"))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var states []nodeStatePayload
	if err = json.NewDecoder(resp.Body).Decode(&states); err == nil {
		for _, s := range states {
			if s.Host == c.host {
				return s.UUID, nil
			}
		}
		return "", errStateNotFound
	}
	return "", err
}

// GetDatasetState performs a get request to get the state of the given datasetID, if
// something goes wrong or the datasetID was not found it returns an error.
func (c flockerClient) GetDatasetState(datasetID string) (*datasetState, error) {
	resp, err := c.get(c.getURL("state/datasets"))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var states []datasetStatePayload
	if err = json.NewDecoder(resp.Body).Decode(&states); err == nil {
		for _, s := range states {
			if s.DatasetID == datasetID {
				return s.datasetState, nil
			}
		}
		return nil, errStateNotFound
	}

	return nil, err
}

/*
CreateVolume creates a volume in Flocker, waits for it to be ready and
returns the dataset id.

This process is a little bit complex but follows this flow:

1. Find the Flocker Control Service UUID
2. Try to create the dataset
3. If it already exists an error is returned
4. If it didn't previously exist, wait for it to be ready
*/
func (c flockerClient) CreateVolume(dir string) (path string, err error) {
	// 1) Find the primary Flocker UUID
	// Note: it could be cached, but doing this query we health check it
	primary, err := c.findPrimaryUUID()
	if err != nil {
		return "", err
	}

	// 2) Try to create the dataset in the given Primary
	datasetID := datasetIDFromName(dir)

	payload := configurationPayload{
		Primary:     primary,
		DatasetID:   datasetID,
		MaximumSize: json.Number(c.maximumSize),
		Metadata: metadataPayload{
			Name: dir,
		},
	}

	resp, err := c.post(c.getURL("configuration/datasets"), payload)
	if err != nil {
		return "", err
	}
	resp.Body.Close() // StatusCodes are enough

	// 3) Return if the dataset was previously created
	if resp.StatusCode == http.StatusConflict {
		return "", errVolumeAlreadyExists
	}

	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("Expected: {1,2}xx creating the volume, got: %d", resp.StatusCode)
	}

	// 4) Wait until the dataset is ready for usage. In case it never gets
	// ready there is a timeoutChan that will return an error
	timeoutChan := time.NewTimer(timeoutWaitingForVolume).C
	tickChan := time.NewTicker(tickerWaitingForVolume).C

	for {
		if s, err := c.GetDatasetState(datasetID); err == nil {
			return s.Path, nil
		} else if err != errStateNotFound {
			return "", err
		}

		select {
		case <-timeoutChan:
			return "", err
		case <-tickChan:
			break
		}
	}
}

func (c flockerClient) LookupVolume(dir string) (path string, err error) {
	var s *datasetState

	datasetID := datasetIDFromName(dir)

	if s, err = c.GetDatasetState(datasetID); err == nil {
		return s.Path, err
	}

	switch err {
	case errStateNotFound:
		return "", errVolumeDoesNotExist
	default:
		return "", err
	}
}

func (c flockerClient) UpdateDatasetPrimary(dir, newPrimary string) error {
	datasetID := datasetIDFromName(dir)

	payload := configurationPayload{
		Primary: newPrimary,
	}

	url := c.getURL(fmt.Sprintf("configuration/datasets/%s", datasetID))
	resp, err := c.post(url, payload)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errUpdatingDataset
	}
	return nil
}
