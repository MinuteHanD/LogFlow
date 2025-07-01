//go:build integration
// +build integration

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os/exec"
	"testing"
	"time"
)

func TestPipeline_Integration(t *testing.T) {

	t.Log("Starting services with docker-compose...")
	cmdUp := exec.Command("docker", "compose", "up", "-d", "--build")
	if err := cmdUp.Run(); err != nil {
		t.Fatalf("Failed to start docker-compose services: %v", err)
	}

	t.Cleanup(func() {
		t.Log("Tearing down services with docker-compose...")
		cmdDown := exec.Command("docker", "compose", "down", "-v")
		if err := cmdDown.Run(); err != nil {
			t.Logf("Failed to tear down docker-compose services: %v", err)
		}
	})

	t.Log("Waiting for services to become healthy...")

	// Poll for Ingestor readiness
	ingestorReady := false
	for i := 0; i < 30; i++ { // Poll for up to 60 seconds (30 * 2s)
		resp, err := http.Get("http://localhost:8081/validation-rules")
		if err == nil && resp.StatusCode == http.StatusOK {
			ingestorReady = true
			resp.Body.Close()
			t.Log("Ingestor service is ready.")
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	if !ingestorReady {
		t.Fatal("Ingestor service did not become ready in time.")
	}

	// Poll for Elasticsearch readiness
	esReady := false
	for i := 0; i < 30; i++ { // Poll for up to 60 seconds (30 * 2s)
		resp, err := http.Get("http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=1s")
		if err == nil && resp.StatusCode == http.StatusOK {
			var healthResp map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&healthResp); err == nil {
				status, ok := healthResp["status"].(string)
				if ok && (status == "green" || status == "yellow") {
					esReady = true
					t.Log("Elasticsearch service is ready.")
					break
				}
			}
			resp.Body.Close()
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	if !esReady {
		t.Fatal("Elasticsearch service did not become ready in time.")
	}

	t.Log("Sending a test log to the ingestor...")
	timestamp := time.Now().UTC().Format(time.RFC3339)
	logPayload := map[string]interface{}{
		"level":     "info",
		"service":   "integration-test",
		"message":   "This is a test log from the integration test.",
		"timestamp": timestamp,
	}
	payloadBytes, _ := json.Marshal(logPayload)

	resp, err := http.Post("http://localhost:8081/log", "application/json", bytes.NewReader(payloadBytes))
	if err != nil {
		t.Fatalf("Failed to send log to ingestor: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		t.Fatalf("Ingestor returned non-OK status: %d, body: %s", resp.StatusCode, string(body))
	}
	resp.Body.Close()
	t.Log("Log sent successfully.")

	t.Log("Verifying the log in Elasticsearch...")

	var found bool

	for i := 0; i < 10; i++ {
		time.Sleep(5 * time.Second)

		esQuery := map[string]interface{}{
			"query": map[string]interface{}{
				"match": map[string]interface{}{
					"message": "This is a test log from the integration test.",
				},
			},
		}
		queryBytes, _ := json.Marshal(esQuery)

		esResp, err := http.Post(fmt.Sprintf("http://localhost:9200/logs-%s/_search", time.Now().UTC().Format("2006.01.02")), "application/json", bytes.NewReader(queryBytes))
		if err != nil {
			t.Logf("Attempt %d: Failed to query Elasticsearch: %v", i+1, err)
			continue
		}

		var esResult map[string]interface{}
		if err := json.NewDecoder(esResp.Body).Decode(&esResult); err != nil {
			esResp.Body.Close()
			t.Logf("Attempt %d: Failed to decode Elasticsearch response: %v", i+1, err)
			continue
		}
		esResp.Body.Close()

		hits, _ := esResult["hits"].(map[string]interface{})["hits"].([]interface{})
		if len(hits) > 0 {
			t.Log("Log found in Elasticsearch!")
			found = true
			break
		}
		t.Logf("Attempt %d: Log not yet found in Elasticsearch. Retrying...", i+1)
	}

	if !found {
		t.Fatal("Verification failed: Log did not appear in Elasticsearch within the time limit.")
	}
}
