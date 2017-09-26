package simplepb

//
// support for PB tester.
//
// we will use the original config.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "labrpc"
import "sync"
import "testing"
import "runtime"
import crand "crypto/rand"
import "encoding/base64"
import "sync/atomic"
import "time"
import "fmt"

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

type config struct {
	mu        sync.Mutex
	t         *testing.T
	net       *labrpc.Network
	n         int
	done      int32 // tell internal threads to die
	pbservers []*PBServer
	applyErr  []string   // from apply channel readers
	connected []bool     // whether each server is on the net
	endnames  [][]string // the port file names each sends to
}

var ncpu_once sync.Once

// n is the total number of servers
func make_config(t *testing.T, n int, unreliable bool) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.applyErr = make([]string, cfg.n)
	cfg.pbservers = make([]*PBServer, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.endnames = make([][]string, cfg.n)

	cfg.setunreliable(unreliable)

	cfg.net.LongDelays(true)

	// create a full set of PBServers
	for i := 0; i < cfg.n; i++ {
		cfg.start1(i)
	}

	// connect everyone
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}

	return cfg
}

// shut down a server but save its persistent state.
func (cfg *config) crash1(i int) {
	cfg.disconnect(i)
	cfg.net.DeleteServer(i) // disable client connections to the server.

	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	srv := cfg.pbservers[i]
	if srv != nil {
		cfg.mu.Unlock()
		srv.Kill()
		cfg.mu.Lock()
		cfg.pbservers[i] = nil
	}
}

//
// make and initialize a server.
func (cfg *config) start1(i int) {
	cfg.crash1(i)

	// a fresh set of outgoing ClientEnd names.
	// so that old crashed instance's ClientEnds can't send.
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	peer := Make(ends, i, 0)

	cfg.mu.Lock()
	cfg.pbservers[i] = peer
	cfg.mu.Unlock()

	svc := labrpc.MakeService(peer)
	srv := labrpc.MakeServer()
	srv.AddService(svc)
	cfg.net.AddServer(i, srv)
}

func (cfg *config) cleanup() {
	for i := 0; i < len(cfg.pbservers); i++ {
		if cfg.pbservers[i] != nil {
			cfg.pbservers[i].Kill()
		}
	}
	atomic.StoreInt32(&cfg.done, 1)
}

// attach server i to the net.
func (cfg *config) connect(i int) {
	// fmt.Printf("connect(%d)\n", i)

	cfg.connected[i] = true

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, true)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.connected[j] {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, true)
		}
	}
}

// detach server i from the net.
func (cfg *config) disconnect(i int) {
	// fmt.Printf("disconnect(%d)\n", i)

	cfg.connected[i] = false

	// outgoing ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][j]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming ClientEnds
	for j := 0; j < cfg.n; j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[j][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) rpcCount(server int) int {
	return cfg.net.GetCount(server)
}

func (cfg *config) setunreliable(unrel bool) {
	cfg.net.Reliable(!unrel)
}

func (cfg *config) setlongreordering(longrel bool) {
	cfg.net.LongReordering(longrel)
}

func (cfg *config) waitCommitted(primary, index int) {
	pri := cfg.pbservers[primary]

	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		committed := pri.IsCommitted(index)
		if committed {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	cfg.t.Fatalf("timed out waiting for index %d to commit\n", index)
}

func (cfg *config) checkCommittedIndex(index int, command interface{}, expectedServers int) {
	cmd, cmdInitialized := command.(int)

	// check that sufficient number of backups committed the same command at index
	nReplicated := 0
	for i := 0; i < len(cfg.pbservers); i++ {
		if cfg.connected[i] {
			ok, command1 := cfg.pbservers[i].GetEntryAtIndex(index)
			if ok {
				cmd1, ok1 := command1.(int)
				if !ok1 {
					continue
				}
				if !cmdInitialized {
					cmd = cmd1
				}
				if cmd1 == cmd {
					nReplicated++
				}
			}
		}
	}
	if nReplicated < expectedServers {
		cfg.t.Fatalf("command %v replicated to %d servers, expected replication to %d servers\n", command, nReplicated, expectedServers)
	}
}

// replicate "cmd" to "expectedServers" number of servers
func (cfg *config) replicateOne(server int, cmd int, expectedServers int) (
	index int) {
	// submit command to primary
	var pri *PBServer
	cfg.mu.Lock()
	pri = cfg.pbservers[server]
	cfg.mu.Unlock()
	index, _, ok := pri.Start(cmd)
	if !ok {
		cfg.t.Fatalf("node-%d rejected command\n", server)
	}
	// primary submitted our request, wait for a while for
	// it to be considered committed and check that it has
	// replicated to sufficient number of servers
	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		committed := pri.IsCommitted(index)
		if committed {
			nReplicated := 0
			for i := 0; i < len(cfg.pbservers); i++ {
				ok, cmd1 := cfg.pbservers[i].GetEntryAtIndex(index)
				if ok {
					if cmd2, ok2 := cmd1.(int); ok2 && cmd2 == cmd {
						nReplicated++
					}
				}
			}
			if nReplicated >= expectedServers {
				return index
			}
		}
		time.Sleep(80 * time.Millisecond)
	}
	cfg.t.Fatalf("timed out replicating cmd (%v) to %d expected servers\n", cmd, expectedServers)
	return -1
}

func (cfg *config) viewChange(newView int) {
	primary := GetPrimary(newView, len(cfg.pbservers))
	if !cfg.connected[primary] {
		cfg.t.Fatalf("node-%d not connected to perform view change\n", primary)
	}
	pri := cfg.pbservers[primary]
	pri.PromptViewChange(newView)
	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		view, ok := pri.ViewStatus()
		if ok && view >= newView {
			return
		}
		time.Sleep(80 * time.Millisecond)
	}
	cfg.t.Fatalf("timed out waiting for view %d change to complete at node-%d\n", newView, primary)
}
