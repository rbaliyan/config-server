package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	configpb "github.com/rbaliyan/config-server/proto/config/v1"
)

type diffResponse struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	V1        int64  `json:"v1"`
	V2        int64  `json:"v2"`
	V1Value   []byte `json:"v1_value,omitempty"`
	V1Codec   string `json:"v1_codec,omitempty"`
	V2Value   []byte `json:"v2_value,omitempty"`
	V2Codec   string `json:"v2_codec,omitempty"`
	Changed   bool   `json:"changed"`
}

func parseDiffParams(r *http.Request) (namespace, key string, v1, v2 int64, err error) {
	namespace = r.PathValue("namespace")
	key = r.PathValue("key")
	if namespace == "" {
		err = errors.New("missing namespace")
		return
	}
	if key == "" {
		err = errors.New("missing key")
		return
	}
	q := r.URL.Query()
	v1, err = strconv.ParseInt(q.Get("v1"), 10, 64)
	if err != nil {
		return
	}
	v2, err = strconv.ParseInt(q.Get("v2"), 10, 64)
	return
}

func fetchVersion(getVersions func(req *configpb.GetVersionsRequest) (*configpb.GetVersionsResponse, error), namespace, key string, version int64) (*configpb.Entry, error) {
	resp, err := getVersions(&configpb.GetVersionsRequest{
		Namespace: namespace,
		Key:       key,
		Version:   version,
		Limit:     1,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.GetEntries()) == 0 {
		return nil, nil
	}
	return resp.GetEntries()[0], nil
}

func newInProcessDiffHandler(svc configpb.ConfigServiceServer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		namespace, key, v1Num, v2Num, err := parseDiffParams(r)
		if err != nil {
			http.Error(w, "invalid parameters: "+err.Error(), http.StatusBadRequest)
			return
		}

		ctx := httpHeadersToMetadata(r.Context(), r)

		getVersions := func(req *configpb.GetVersionsRequest) (*configpb.GetVersionsResponse, error) {
			return svc.GetVersions(ctx, req)
		}

		e1, err := fetchVersion(getVersions, namespace, key, v1Num)
		if err != nil {
			writeHTTPError(w, err)
			return
		}
		if e1 == nil {
			http.Error(w, "version not found", http.StatusNotFound)
			return
		}

		e2, err := fetchVersion(getVersions, namespace, key, v2Num)
		if err != nil {
			writeHTTPError(w, err)
			return
		}
		if e2 == nil {
			http.Error(w, "version not found", http.StatusNotFound)
			return
		}

		resp := diffResponse{
			Namespace: namespace,
			Key:       key,
			V1:        v1Num,
			V2:        v2Num,
			V1Value:   e1.GetValue(),
			V1Codec:   e1.GetCodec(),
			V2Value:   e2.GetValue(),
			V2Codec:   e2.GetCodec(),
			Changed:   !bytes.Equal(e1.GetValue(), e2.GetValue()) || e1.GetCodec() != e2.GetCodec(),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}

func newRemoteDiffHandler(client configpb.ConfigServiceClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		namespace, key, v1Num, v2Num, err := parseDiffParams(r)
		if err != nil {
			http.Error(w, "invalid parameters: "+err.Error(), http.StatusBadRequest)
			return
		}

		ctx := httpHeadersToMetadata(r.Context(), r)

		getVersions := func(req *configpb.GetVersionsRequest) (*configpb.GetVersionsResponse, error) {
			return client.GetVersions(ctx, req)
		}

		e1, err := fetchVersion(getVersions, namespace, key, v1Num)
		if err != nil {
			writeHTTPError(w, err)
			return
		}
		if e1 == nil {
			http.Error(w, "version not found", http.StatusNotFound)
			return
		}

		e2, err := fetchVersion(getVersions, namespace, key, v2Num)
		if err != nil {
			writeHTTPError(w, err)
			return
		}
		if e2 == nil {
			http.Error(w, "version not found", http.StatusNotFound)
			return
		}

		resp := diffResponse{
			Namespace: namespace,
			Key:       key,
			V1:        v1Num,
			V2:        v2Num,
			V1Value:   e1.GetValue(),
			V1Codec:   e1.GetCodec(),
			V2Value:   e2.GetValue(),
			V2Codec:   e2.GetCodec(),
			Changed:   !bytes.Equal(e1.GetValue(), e2.GetValue()) || e1.GetCodec() != e2.GetCodec(),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})
}
