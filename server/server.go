package server

import (
	"net/http"
	"io"
	"log"
	"net"
)

type Server struct {

}

const (
	Http_Path_Rpc = "/_kittenRpc_"
	Http_Path_Debug = "/debug/kittenRpc"
)

func NewServer() *Server {
	return &Server{}
}

// handle http
func (server *Server) HandleHttp(rpcPath string, debugPath string) {
	http.Handle(rpcPath, server)
	http.Handle(debugPath, server)
}

var connected = "200 Connected to Go RPC"

// ServeHTTP implements an http.Handle
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request)  {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}

// Serve Conn
func (server *Server) ServeConn(conn net.Conn) {

}