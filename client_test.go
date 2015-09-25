package flocker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/kubernetes/pkg/volume"

	"github.com/stretchr/testify/assert"
)

func TestMaximumSizeIs1024Multiple(t *testing.T) {
	assert := assert.New(t)

	n, err := strconv.Atoi(string(defaultVolumeSize))
	assert.NoError(err)
	assert.Equal(0, n%1024)
}

func TestPost(t *testing.T) {
	const (
		expectedPayload    = "foobar"
		expectedStatusCode = 418
	)

	assert := assert.New(t)

	type payload struct {
		Test string `json:"test"`
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var result payload
		err := json.NewDecoder(r.Body).Decode(&result)
		assert.NoError(err)
		assert.Equal(expectedPayload, result.Test)
		w.WriteHeader(expectedStatusCode)
	}))
	defer ts.Close()

	c := flockerClient{Client: &http.Client{}}

	resp, err := c.post(ts.URL, payload{expectedPayload})
	assert.NoError(err)
	assert.Equal(expectedStatusCode, resp.StatusCode)
}

func TestGet(t *testing.T) {
	const (
		expectedStatusCode = 418
	)

	assert := assert.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(expectedStatusCode)
	}))
	defer ts.Close()

	c := flockerClient{Client: &http.Client{}}

	resp, err := c.get(ts.URL)
	assert.NoError(err)
	assert.Equal(expectedStatusCode, resp.StatusCode)
}

func TestFindIDInConfigurationsPayload(t *testing.T) {
	const (
		searchedName = "search-for-this-name"
		expected     = "The-42-id"
	)
	assert := assert.New(t)

	c := flockerClient{}

	payload := fmt.Sprintf(
		`[{"dataset_id": "1-2-3", "metadata": {"name": "test"}}, {"dataset_id": "The-42-id", "metadata": {"name": "%s"}}]`,
		searchedName,
	)

	id, err := c.findIDInConfigurationsPayload(
		ioutil.NopCloser(bytes.NewBufferString(payload)), searchedName,
	)
	assert.NoError(err)
	assert.Equal(expected, id)

	id, err = c.findIDInConfigurationsPayload(
		ioutil.NopCloser(bytes.NewBufferString(payload)), "it will not be found",
	)
	assert.Equal(errConfigurationNotFound, err)

	id, err = c.findIDInConfigurationsPayload(
		ioutil.NopCloser(bytes.NewBufferString("invalid { json")), "",
	)
	assert.Error(err)
}

func TestFindPathInDatasetStatePayload(t *testing.T) {
	const (
		searchedID = "search-for-this-dataset-id"
		expected   = "awesome-path"
	)
	assert := assert.New(t)

	c := flockerClient{}

	payload := fmt.Sprintf(
		`[{"dataset_id": "1-2-3", "path": "not-this-one"}, {"dataset_id": "%s", "path": "awesome-path"}]`,
		searchedID,
	)
	path, err := c.findPathInDatasetStatePayload(
		ioutil.NopCloser(bytes.NewBufferString(payload)), searchedID,
	)
	assert.NoError(err)
	assert.Equal(expected, path)

	path, err = c.findPathInDatasetStatePayload(
		ioutil.NopCloser(bytes.NewBufferString(payload)), "this is not going to be there",
	)
	assert.Equal(errStateNotFound, err)

	path, err = c.findPathInDatasetStatePayload(
		ioutil.NopCloser(bytes.NewBufferString("not even } json")), "",
	)
	assert.Error(err)
}

func TestFindPrimaryUUID(t *testing.T) {
	const expectedPrimary = "primary-uuid"
	assert := assert.New(t)

	var (
		mockedHost    = "127.0.0.1"
		mockedPrimary = expectedPrimary
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("GET", r.Method)
		assert.Equal("/v1/state/nodes", r.URL.Path)
		w.Write([]byte(fmt.Sprintf(`[{"host": "%s", "uuid": "%s"}]`, mockedHost, mockedPrimary)))
	}))

	host, port, err := getHostAndPortFromTestServer(ts)
	assert.NoError(err)

	c := newFlockerTestClient(host, port)
	assert.NoError(err)
	c.schema = "http"

	mockedPrimary = expectedPrimary
	primary, err := c.findPrimaryUUID()
	assert.NoError(err)
	assert.Equal(expectedPrimary, primary)

	mockedHost = "not.found"
	_, err = c.findPrimaryUUID()
	assert.Equal(errStateNotFound, err)
}

func TestGetURL(t *testing.T) {
	const (
		expectedHost = "host"
		expectedPort = 42
	)

	assert := assert.New(t)

	c := newFlockerTestClient(expectedHost, expectedPort)
	var expectedURL = fmt.Sprintf("%s://%s:%d/v1/test", c.schema, expectedHost, expectedPort)

	url := c.getURL("test")
	assert.Equal(expectedURL, url)
}

func getHostAndPortFromTestServer(ts *httptest.Server) (string, int, error) {
	tsURL, err := url.Parse(ts.URL)
	if err != nil {
		return "", 0, err
	}

	hostSplits := strings.Split(tsURL.Host, ":")

	port, err := strconv.Atoi(hostSplits[1])
	if err != nil {
		return "", 0, nil
	}
	return hostSplits[0], port, nil
}

func getVolumeConfig(host string, port int) volume.VolumeConfig {
	return volume.VolumeConfig{
		OtherAttributes: map[string]string{
			"CONTROL_SERVICE_HOST": host,
			"CONTROL_SERVICE_PORT": strconv.Itoa(port),
		},
	}
}

func TestHappyPathCreateVolumeFromNonExistent(t *testing.T) {
	const (
		expectedDatasetName = "dir"
		expectedPrimary     = "A-B-C-D"
	)
	expectedDatasetID := datasetIDFromName(expectedDatasetName)

	assert := assert.New(t)
	var (
		numCalls int
		err      error
	)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		numCalls++
		switch numCalls {
		case 1:
			assert.Equal("GET", r.Method)
			assert.Equal("/v1/state/nodes", r.URL.Path)
			w.Write([]byte(fmt.Sprintf(`[{"host": "127.0.0.1", "uuid": "%s"}]`, expectedPrimary)))
		case 2:
			assert.Equal("POST", r.Method)
			assert.Equal("/v1/configuration/datasets", r.URL.Path)

			var c configurationPayload
			err := json.NewDecoder(r.Body).Decode(&c)
			assert.NoError(err)
			assert.Equal(expectedPrimary, c.Primary)
			assert.Equal(defaultVolumeSize, c.MaximumSize)
			assert.Equal(expectedDatasetName, c.Metadata.Name)
			assert.Equal(expectedDatasetID, c.DatasetID)

			w.Write([]byte(fmt.Sprintf(`{"dataset_id": "%s"}`, expectedDatasetID)))
		case 3:
			assert.Equal("GET", r.Method)
			assert.Equal("/v1/state/datasets", r.URL.Path)
		case 4:
			assert.Equal("GET", r.Method)
			assert.Equal("/v1/state/datasets", r.URL.Path)
			w.Write([]byte(fmt.Sprintf(`[{"dataset_id": "%s", "path": "%s"}]`, expectedDatasetID, expectedDatasetName)))
		}
	}))

	host, port, err := getHostAndPortFromTestServer(ts)
	assert.NoError(err)

	c := newFlockerTestClient(host, port)
	assert.NoError(err)
	c.schema = "http"
	tickerWaitingForVolume = 1 * time.Millisecond // TODO: this is overriding globally

	datasetID, err := c.CreateVolume(expectedDatasetName)
	assert.NoError(err)
	assert.Equal(expectedDatasetID, datasetID)
}

func TestCreateVolumeThatAlreadyExists(t *testing.T) {
	const (
		expectedPrimary     = "A-B-C-D"
		expectedDatasetName = "dir"
	)
	expectedDatasetID := datasetIDFromName(expectedDatasetName)

	assert := assert.New(t)
	var numCalls int

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		numCalls++
		switch numCalls {
		case 1:
			assert.Equal("GET", r.Method)
			assert.Equal("/v1/state/nodes", r.URL.Path)
			w.Write([]byte(fmt.Sprintf(`[{"host": "127.0.0.1", "uuid": "%s"}]`, expectedPrimary)))
		case 2:
			assert.Equal("POST", r.Method)
			assert.Equal("/v1/configuration/datasets", r.URL.Path)
			w.WriteHeader(http.StatusConflict)
		}
	}))

	host, port, err := getHostAndPortFromTestServer(ts)
	assert.NoError(err)

	c := newFlockerTestClient(host, port)
	assert.NoError(err)
	c.schema = "http"

	datasetID, err := c.CreateVolume(expectedDatasetName)
	assert.NoError(err)
	assert.Equal(expectedDatasetID, datasetID)
}

func newFlockerTestClient(host string, port int) *flockerClient {
	return &flockerClient{
		Client:      &http.Client{},
		host:        host,
		port:        port,
		version:     "v1",
		schema:      "http",
		maximumSize: defaultVolumeSize,
	}
}
