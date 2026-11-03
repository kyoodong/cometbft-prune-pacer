package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// tx_search returns at most 100 txs per page (CometBFT maxPerPage). With 150
// txs at one height the witness must still hold all of them, or waitGone can
// finish while 50 are left.
func TestTxHashesAtReadsAllPages(t *testing.T) {
	const n = 150
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		per, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
		per = min(max(per, 1), 100)
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		page = max(page, 1)
		pages := (n + per - 1) / per
		if page > pages { // like CometBFT validatePage
			fmt.Fprintf(w, `{"error":{"code":-32603,"message":"Internal error","data":"page should be within [1, %d] range, given %d"}}`, pages, page)
			return
		}
		var hs []string
		for i := (page - 1) * per; i < min(page*per, n); i++ {
			hs = append(hs, fmt.Sprintf(`{"hash":"H%d"}`, i))
		}
		fmt.Fprintf(w, `{"result":{"txs":[%s],"total_count":"%d"}}`, strings.Join(hs, ","), n)
	}))
	defer ts.Close()
	node, _ := dialNode(&Config{GRPCAddr: "127.0.0.1:1", RPCAddr: ts.URL})
	hs, err := node.TxHashesAt(context.Background(), 1299)
	if err != nil || len(hs) != n || hs[n-1] != fmt.Sprintf("H%d", n-1) {
		t.Fatalf("got %d hashes, err=%v; want %d", len(hs), err, n)
	}
}
