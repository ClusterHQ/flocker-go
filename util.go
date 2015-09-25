package flocker

import (
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"

	"github.com/pborman/uuid"
)

// datasetIDFromName will return a UUID string for the input value.
func datasetIDFromName(v string) string {
	space := uuid.NIL
	return uuid.NewHash(md5.New(), space, []byte(v), 4).String()
}

// newTLSClient returns a new TLS http client
func newTLSClient(caCertPath, keyPath, certPath string) (*http.Client, error) {
	// Client certificate
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}

	// CA certificate
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}

	return &http.Client{Transport: transport}, nil
}
