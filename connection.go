package redisnats

import (
	"bufio"
	"log/slog"
	"net"

	"github.com/henomis/redis2nats/nats"
)

type connection struct {
	conn        net.Conn
	storagePool []*nats.KV
	log         *slog.Logger
}

func NewConnection(conn net.Conn, storagePool []*nats.KV) *connection {
	return &connection{
		conn:        conn,
		storagePool: storagePool,
		log:         slog.Default().With("module", "redis-connection"),
	}
}

// handle processes each incoming connection.
func (c *connection) handle() {
	defer c.conn.Close()

	c.log.Info("New connection", "address", c.conn.RemoteAddr())

	commandExecutor := NewCommandExecutor(c.storagePool)

	reader := bufio.NewReader(c.conn)
	for {
		response, err := commandExecutor.Execute(reader)
		if err != nil && err.Error() == "EOF" {
			c.log.Info("Connection closed", "address", c.conn.RemoteAddr())
			return
		} else if err != nil {
			c.log.Error("Error processing command", "error", err)
			err = c.writeError(c.conn, "ERR "+err.Error())
			if err != nil {
				c.log.Error("Error writing error message", "error", err)
				return
			}
		} else {
			err := c.writeResponse(c.conn, response)
			if err != nil {
				c.log.Error("Error writing response", "error", err)
				return
			}
		}
	}
}

// writeResponse writes a simple Redis RESP message.
func (c *connection) writeResponse(conn net.Conn, message string) error {
	_, err := conn.Write([]byte(message + "\r\n"))
	return err
}

// writeError writes an error message to the client.
func (c *connection) writeError(conn net.Conn, message string) error {
	_, err := conn.Write([]byte("-" + message + "\r\n"))
	return err
}
