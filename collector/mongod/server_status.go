// Copyright 2017 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mongod

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/ghostbaby/mongodb_exporter/collector/common"
	"fmt"
	"time"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"strings"
	"go.mongodb.org/mongo-driver/x/network/connstring"
)

// ServerStatus keeps the data returned by the serverStatus() method.
type ServerStatus struct {
	collector_common.ServerStatus `bson:",inline"`

	Dur *DurStats `bson:"dur"`

	BackgroundFlushing *FlushStats `bson:"backgroundFlushing"`

	GlobalLock *GlobalLockStats `bson:"globalLock"`

	IndexCounter *IndexCounterStats `bson:"indexCounters"`

	Locks LockStatsMap `bson:"locks,omitempty"`

	OpLatencies *OpLatenciesStat `bson:"opLatencies"`
	Metrics     *MetricsStats    `bson:"metrics"`

	StorageEngine *StorageEngineStats `bson:"storageEngine"`
	InMemory      *WiredTigerStats    `bson:"inMemory"`
	RocksDb       *RocksDbStats       `bson:"rocksdb"`
	WiredTiger    *WiredTigerStats    `bson:"wiredTiger"`
}


// Export exports the server status to be consumed by prometheus.
func (status *ServerStatus) Export(ch chan<- prometheus.Metric) {
	status.ServerStatus.Export(ch)
	if status.Dur != nil {
		status.Dur.Export(ch)
	}
	if status.BackgroundFlushing != nil {
		status.BackgroundFlushing.Export(ch)
	}
	if status.GlobalLock != nil {
		status.GlobalLock.Export(ch)
	}
	if status.IndexCounter != nil {
		status.IndexCounter.Export(ch)
	}
	if status.OpLatencies != nil {
		status.OpLatencies.Export(ch)
	}
	if status.Locks != nil {
		status.Locks.Export(ch)
	}
	if status.Metrics != nil {
		status.Metrics.Export(ch)
	}
	if status.InMemory != nil {
		status.InMemory.Export(ch)
	}
	if status.RocksDb != nil {
		status.RocksDb.Export(ch)
	}
	if status.WiredTiger != nil {
		status.WiredTiger.Export(ch)
	}
	// If db.serverStatus().storageEngine does not exist (3.0+ only) and status.BackgroundFlushing does (MMAPv1 only), default to mmapv1
	// https://docs.mongodb.com/v3.0/reference/command/serverStatus/#storageengine
	if status.StorageEngine == nil && status.BackgroundFlushing != nil {
		status.StorageEngine = &StorageEngineStats{
			Name: "mmapv1",
		}
	}
	if status.StorageEngine != nil {
		status.StorageEngine.Export(ch)
	}
}

// Describe describes the server status for prometheus.
func (status *ServerStatus) Describe(ch chan<- *prometheus.Desc) {
	status.ServerStatus.Describe(ch)
	if status.Dur != nil {
		status.Dur.Describe(ch)
	}
	if status.BackgroundFlushing != nil {
		status.BackgroundFlushing.Describe(ch)
	}
	if status.GlobalLock != nil {
		status.GlobalLock.Describe(ch)
	}
	if status.IndexCounter != nil {
		status.IndexCounter.Describe(ch)
	}
	if status.OpLatencies != nil {
		status.OpLatencies.Describe(ch)
	}
	if status.Opcounters != nil {
		status.Opcounters.Describe(ch)
	}
	if status.OpcountersRepl != nil {
		status.OpcountersRepl.Describe(ch)
	}
	if status.Locks != nil {
		status.Locks.Describe(ch)
	}
	if status.Metrics != nil {
		status.Metrics.Describe(ch)
	}
	if status.StorageEngine != nil {
		status.StorageEngine.Describe(ch)
	}
	if status.InMemory != nil {
		status.InMemory.Describe(ch)
	}
	if status.RocksDb != nil {
		status.RocksDb.Describe(ch)
	}
	if status.WiredTiger != nil {
		status.WiredTiger.Describe(ch)
	}
}

// GetServerStatus returns the server status info.
func GetServerStatus(client *mongo.Client) *ServerStatus {
	result := &ServerStatus{}
	err := client.Database("admin").RunCommand(context.TODO(), bson.D{
		{Key: "serverStatus", Value: 1},
		{Key: "recordStats", Value: 0},
		{Key: "opLatencies", Value: bson.M{"histograms": true}},
	}).Decode(result)
	if err != nil {
		log.Errorf("Failed to get server status: %s", err)
		return nil
	}

	return result
}

type MongoSessionOpts struct {
	URI                   string
	TLSConnection         bool
	TLSCertificateFile    string
	TLSPrivateKeyFile     string
	TLSCaFile             string
	TLSHostnameValidation bool
	PoolLimit             int
	SocketTimeout         time.Duration
	SyncTimeout           time.Duration
	AuthentificationDB    string
}

// MongoClient connects to MongoDB and returns ready to use MongoDB client.
func MongoClient(opts *MongoSessionOpts) *mongo.Client {
	if strings.Contains(opts.URI, "ssl=true") {
		opts.URI = strings.Replace(opts.URI, "ssl=true", "", 1)
		opts.TLSConnection = true
	}

	cOpts := options.Client().
		ApplyURI(opts.URI).
		SetDirect(true).
		SetSocketTimeout(opts.SocketTimeout).
		SetConnectTimeout(opts.SyncTimeout).
		SetMaxPoolSize(uint16(opts.PoolLimit)).
		SetReadPreference(readpref.Nearest())

	if cOpts.Auth != nil {
		cOpts.Auth.AuthSource = opts.AuthentificationDB
	}

	//err := opts.configureDialInfoIfRequired(cOpts)
	//if err != nil {
	//	log.Errorf("%s", err)
	//	return nil
	//}

	client, err := mongo.NewClient(cOpts)
	if err != nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.SyncTimeout)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Errorf("Cannot connect to server using url %s: %s", RedactMongoUri(opts.URI), err)
		return nil
	}

	return client
}

// RedactMongoUri removes login and password from mongoUri.
func RedactMongoUri(uri string) string {
	if strings.HasPrefix(uri, "mongodb://") && strings.Contains(uri, "@") {
		if strings.Contains(uri, "ssl=true") {
			uri = strings.Replace(uri, "ssl=true", "", 1)
		}

		cStr, err := connstring.Parse(uri)
		if err != nil {
			log.Errorf("Cannot parse mongodb server url: %s", err)
			return "unknown/error"
		}

		if cStr.Username != "" && cStr.Password != "" {
			uri = strings.Replace(uri, cStr.Username, "****", 1)
			uri = strings.Replace(uri, cStr.Password, "****", 1)
			return uri
		}
	}
	return uri
}

// TestConnection connects to MongoDB and returns BuildInfo.
func TestServerStatus(opts MongoSessionOpts) ( error) {
	client := MongoClient(&opts)
	if client == nil {
		return fmt.Errorf("Cannot connect using uri: %s", opts.URI)
	}
	err := GetServerStatus(client)
	if err == nil {
		return fmt.Errorf("Cannot get Server Status for MongoDB using uri %s", opts.URI)
	}

	return nil
}
