package model

type RPCResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	Data   []byte `json:"data,omitempty"`
}
