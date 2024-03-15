package sls

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

type credentialProvider struct {
	command   string
	arguments []string
	timeout   int
}

type stsCredential struct {
	AccessKeyId     string `json:"AccessKeyId"`
	AccessKeySecret string `json:"AccessKeySecret"`
	SecurityToken   string `json:"SecurityToken"`
	Expiration      string `json:"Expiration"`
}

func newCredentialProvider(command string, arguments []string, timeout int) *credentialProvider {
	return &credentialProvider{
		command:   command,
		arguments: arguments,
		timeout:   timeout,
	}
}

func (c *credentialProvider) GetCredentials() (accessKeyId string, accessKeySecret string, securityToken string, expiration time.Time, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.timeout)*time.Second)
	defer cancel()

	var stdout strings.Builder
	cmd := exec.CommandContext(ctx, c.command, c.arguments...)
	cmd.Stdout = &stdout

	err = cmd.Run()
	if err != nil {
		return "", "", "", time.Time{}, fmt.Errorf("run credential provider command failed: %w", err)
	}

	var sts stsCredential
	err = json.Unmarshal([]byte(stdout.String()), &sts)
	if err != nil {
		fmt.Printf("stdout: %s\n", stdout.String())
		return "", "", "", time.Time{}, fmt.Errorf("unmarshal sts credential failed: %w", err)
	}

	expiration, err = time.Parse(time.RFC3339, sts.Expiration)
	if err != nil {
		return "", "", "", time.Time{}, fmt.Errorf("parse sts credential expiration failed: %w", err)
	}

	return sts.AccessKeyId, sts.AccessKeySecret, sts.SecurityToken, expiration, nil
}
