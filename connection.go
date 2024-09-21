package redisnats

import (
	"bufio"
	"log/slog"
	"net"
	"time"

	"github.com/henomis/redis2nats/nats"
)

type Connection struct {
	conn        net.Conn
	storagePool []*nats.KV
	natsTimeout time.Duration
	log         *slog.Logger
}

func NewConnection(conn net.Conn, storagePool []*nats.KV, natsTimeout time.Duration) *Connection {
	return &Connection{
		conn:        conn,
		storagePool: storagePool,
		natsTimeout: natsTimeout,
		log:         slog.Default().With("module", "redis-connection"),
	}
}

// handle processes each incoming connection.
func (c *Connection) handle() {
	defer c.conn.Close()

	c.log.Info("New connection", "address", c.conn.RemoteAddr())

	commandExecutor := NewCommandExecutor(c.storagePool, c.natsTimeout)

	reader := bufio.NewReader(c.conn)
	for {
		response, err := commandExecutor.Execute(reader)
		if err != nil && err.Error() == "EOF" {
			c.log.Info("Connection closed", "address", c.conn.RemoteAddr())
			return
		} else if err != nil {
			c.log.Error("Error processing command", "error", err)
			errWrite := c.writeError(c.conn, err)
			if errWrite != nil {
				c.log.Error("Error writing error message", "error", errWrite)
				return
			}
		} else {
			errWrite := c.writeResponse(c.conn, response)
			if errWrite != nil {
				c.log.Error("Error writing response", "error", errWrite)
				return
			}
		}
	}
}

// writeResponse writes a simple Redis RESP message.
func (c *Connection) writeResponse(conn net.Conn, message string) error {
	_, err := conn.Write([]byte(message))
	return err
}

// writeError writes an error message to the client.
func (c *Connection) writeError(conn net.Conn, cmdErr error) error {
	_, err := conn.Write([]byte(fmtSimpleError(cmdErr.Error())))
	return err
}
