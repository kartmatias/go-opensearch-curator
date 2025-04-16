package main

import (
	"context"
	"fmt"
	"kartmatias/go-opensearch-curator/opensearchmanager"
	"log"
)

func main() {
	exemplo1()
	exemplo2()

}

func exemplo1() {
	ctx := context.Background()
	client := opensearchmanager.NewClient("http://localhost:9200", "admin", "adminpassword")

	// Exemplo: Limpeza de índices antigos
	if err := client.CleanupByAge(ctx, "logs-", 30); err != nil {
		log.Fatalf("Failed to cleanup old indices: %v", err)
	}

	// Exemplo: Rollover de índice
	conditions := map[string]interface{}{
		"max_age":  "7d",
		"max_docs": 1000000,
	}
	if err := client.Rollover(ctx, "logs-current", conditions); err != nil {
		log.Fatalf("Failed to rollover index: %v", err)
	}
}

func exemplo2() {
	ctx := context.Background()
	client := opensearchmanager.NewClient("http://localhost:9200", "admin", "adminpassword")

	settings := map[string]interface{}{
		"number_of_shards":   1, // Reduzindo para 1 shard
		"number_of_replicas": 1, // Réplicas serão restauradas após o shrink
	}

	err := client.ShrinkIndex(ctx, "logs-2023.10.01", "logs-2023.10.01-shrink", settings)
	if err != nil {
		log.Fatalf("Failed to shrink index: %v", err)
	}

	fmt.Println("Index shrunk successfully!")
}
