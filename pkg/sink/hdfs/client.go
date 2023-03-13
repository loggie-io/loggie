/*
Copyright 2023 Loggie Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hdfs

import (
	"errors"
	"github.com/colinmarc/hdfs/v2"
	"net"
	"os"
	"path"
	"sync"
	"time"
)

type Client struct {
	filename string

	cli *hdfs.Client

	writer *hdfs.FileWriter

	mu sync.Mutex
}

func makeClient(nameNode []string, filename string) (*Client, error) {
	// TODO use connection pool
	//cli, err := hdfs.New(nameNode)
	dialFunc := (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext

	options := hdfs.ClientOptions{
		Addresses:        nameNode,
		User:             "root",
		NamenodeDialFunc: dialFunc,
		DatanodeDialFunc: dialFunc,
	}

	cli, err := hdfs.NewClient(options)
	if err != nil {
		return nil, err
	}

	return &Client{
		filename: filename,
		cli:      cli,
	}, nil
}

func (c *Client) Write(p []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.filename == "" {
		return 0, errors.New("write to hdfs, filename is empty")
	}

	// init new file writer
	if c.writer == nil {
		_, err := c.cli.Stat(c.filename)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return 0, err
			}

			if err := c.cli.MkdirAll(path.Dir(c.filename), os.ModeDir|0777); err != nil {
				return 0, err
			}

			// create file
			c.writer, err = c.cli.Create(c.filename)
			if err != nil {
				return 0, err
			}
			n, err := c.writer.Write(p)
			if err != nil {
				return 0, err
			}
			if err := c.writer.Flush(); err != nil {
				return 0, err
			}

			return n, nil
		}
	}

	c.writer, err = c.cli.Append(c.filename)
	if err != nil {
		return 0, err
	}

	wn, err := c.writer.Write(p)
	if err != nil {
		return 0, err
	}
	return wn, nil
}

func (c *Client) openExistingOrNew() {

}

// Close implements io.Closer, and closes the current logfile.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.close()
}

// close closes the file if it is open.
func (c *Client) close() error {
	if c.writer == nil {
		return nil
	}

	// TODO use connection pool
	err := c.writer.Close()
	if err != nil {
		return err
	}
	err = c.cli.Close()
	if err != nil {
		return err
	}
	c.writer = nil
	return nil
}
