package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastrand"
	"go.uber.org/atomic"
)

type Card struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

var rate atomic.Int64

const (
	totalUsers = 10000
	totalCards = 100
)

func init() {
	rand.Seed(time.Now().Unix())
	debug.SetGCPercent(2000)
}
func main() {
	cards := getCardsList()
	cmds := startNodeProccess()
	defer func() {
		for _, cmd := range cmds {
			cmd.Process.Kill()
		}
	}()
	waitForNodeProccess()
	store := generateCardsStore(cards)
	userAllCardsStore := generateAllCardsStore(store)
	flushRedis()
	ticker := time.NewTicker(time.Second)
	go startRequestCounter(ticker)
	t := time.Now()
	userIds := scanUsers(store)
	queryResults := queryUsers(userIds)
	parseResults(store, userAllCardsStore, queryResults)
	timePassed := time.Since(t)
	ticker.Stop()
	fmt.Printf(`test took: %d milliseconds| %0.2f seconds`, timePassed.Milliseconds(), timePassed.Seconds())
}

func flushRedis() *redis.StatusCmd {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	}).FlushDB(context.Background())
}

func startRequestCounter(ticker *time.Ticker) {
	for range ticker.C {
		fmt.Printf("%d requests/second\n", rate.Swap(0))
	}
}

func generateCardsStore(cards []Card) *sync.Map {
	fmt.Println("generating in memory store")
	store := &sync.Map{}
	for i := 0; i < totalUsers; i++ {
		usersCards := sync.Map{}
		for _, v := range cards {
			usersCards.Store(v.ID, atomic.NewInt32(0))
		}
		store.Store(uuid.NewString(), &usersCards)
	}
	return store
}

func generateAllCardsStore(store *sync.Map) *sync.Map {
	fmt.Println("generating in memory all cards store")
	usersAllCardsStore := sync.Map{}
	store.Range(func(key, value interface{}) bool {
		usersAllCardsStore.Store(key, atomic.NewInt32(0))
		return true
	})
	return &usersAllCardsStore
}

func waitForNodeProccess() {
	fmt.Println("waiting for node process to boot")
	ok := false
	for i := 10; i > 0; i-- {
		fmt.Print(`.`)
		resp, err := http.Get(`http://localhost:4001/ready`)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == 200 {
			ok = true
			break
		}
		time.Sleep(time.Second)
	}

	if !ok {
		panic(fmt.Errorf("not up"))
	}
	fmt.Println()
}

func startNodeProccess() []*exec.Cmd {
	fmt.Println("starting node processes")
	cmd := exec.Command(`node`, `index.js`, `4001`)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout
	go func() {
		err := cmd.Run()
		if err != nil {
			panic(err)
		}
	}()
	cmd2 := exec.Command(`node`, `index.js`, `4002`)
	cmd2.Stdout = os.Stdout
	cmd2.Stderr = os.Stdout
	go func() {
		err := cmd2.Run()
		if err != nil {
			panic(err)
		}
	}()
	return []*exec.Cmd{cmd, cmd2}
}

func getCardsList() []Card {
	fmt.Println("generating cards")
	cards := make([]Card, 0, totalCards)
	for i := 0; i < cap(cards); i++ {
		cards = append(cards, Card{
			ID:   uuid.NewString(),
			Name: uuid.NewString(),
		})
	}
	b, _ := json.Marshal(cards)
	if err := ioutil.WriteFile(`cards.json`, b, 0777); err != nil {
		fmt.Println(err)
	}
	return cards
}

func scanUsers(results *sync.Map) chan string {
	output := make(chan string, 100)
	go func() {
		defer close(output)
		c := 1
		for c > 0 {
			c = 0
			results.Range(func(key, value interface{}) bool {
				output <- key.(string)
				c++
				return true
			})
			if c == 0 {
				break
			}
		}
		fmt.Println(`done scanning`)
	}()
	return output
}

type result struct {
	userID string
	cardID string
}

func queryUsers(ids chan string) chan result {
	output := make(chan result, 100)
	go func() {
		defer close(output)
		wg := &sync.WaitGroup{}
		for i := 0; i < 128; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for uid := range ids {
					if res, err := makeRequest(uid); err == nil {
						output <- res
					} else {
						if err == io.EOF || strings.HasPrefix(err.Error(), `the server closed connection before`) || strings.HasPrefix(err.Error(), `pipeline connection has been stopped`) {
							continue
						}
						fmt.Println(err)
					}
				}
			}()
		}
		wg.Wait()
		fmt.Println(`done quering`)
	}()
	return output
}

func parseResults(store *sync.Map, userAllCardsStore *sync.Map, results chan result) {
	wg := &sync.WaitGroup{}
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for res := range results {
				if res.cardID == `ALL CARDS` {
					checkAllCards(store, userAllCardsStore, res.userID)
					continue
				}
				addCards(store, res)
			}
		}()
	}
	wg.Wait()

	fmt.Println(`done parsing`)
	store.Range(func(key, value interface{}) bool {
		fmt.Println("still have users left with cards missing")
		return false
	})

	userAllCardsStore.Range(func(key, value interface{}) bool {
		if value.(*atomic.Int32).Load() > 5 {
			fmt.Println("some users got ALL CARDS more than five times")
			return false
		}
		return true
	})
}

func checkAllCards(store *sync.Map, userAllCardsStore *sync.Map, uid string) {
	data, ok := store.Load(uid)
	if !ok {
		return
	}

	defer func() {
		counter, ok := userAllCardsStore.Load(uid)
		if !ok {
			fmt.Println(uid, "all cards for not found")
		}

		counter.(*atomic.Int32).Inc()
	}()

	allCardsPresent := true
	data.(*sync.Map).Range(func(key, value interface{}) bool {
		if value.(*atomic.Int32).Load() != 1 {
			allCardsPresent = false
			return false
		}
		return true
	})
	if !allCardsPresent {
		return
	}
	store.Delete(uid)
	return
}

func addCards(store *sync.Map, res result) {
	data, ok := store.Load(res.userID)
	if !ok {
		fmt.Println(res)
		return
	}
	cardCounter, ok := data.(*sync.Map).Load(res.cardID)
	if !ok {
		fmt.Println(fmt.Errorf("card not found %s", res.cardID))
		return
	}
	if cardCounter.(*atomic.Int32).Inc() > 1 {
		fmt.Println(fmt.Errorf(`got card twice %v`, res))
	}
}

var clients = []fasthttp.PipelineClient{fasthttp.PipelineClient{
	Addr:                          `localhost:4001`,
	NoDefaultUserAgentHeader:      true,
	DisablePathNormalizing:        true,
	DisableHeaderNamesNormalizing: true,
	ReadTimeout:                   time.Second * 5,
},
	fasthttp.PipelineClient{
		Addr:                          `localhost:4002`,
		NoDefaultUserAgentHeader:      true,
		DisablePathNormalizing:        true,
		DisableHeaderNamesNormalizing: true,
		ReadTimeout:                   time.Second * 5,
	}}

func makeRequest(id string) (result, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	r := fastrand.Uint32() % 2
	req.SetRequestURI(fmt.Sprintf("http://localhost:%d/card_add?id=%s", r+1+4000, id))
	if err := clients[r].Do(req, resp); err != nil {
		return result{}, err
	}
	rate.Add(1)
	var card Card
	err := json.Unmarshal(resp.Body(), &card)
	if err != nil {
		fmt.Println(string(resp.Body()))
	}
	return result{userID: id, cardID: card.ID}, err
}
