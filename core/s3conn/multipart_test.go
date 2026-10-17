package s3conn

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeS3 implements just the multipart protocol (and plain PUT/COPY/HEAD) over
// path-style URLs, recording what it receives.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	uploads map[string]map[int][]byte // uploadId -> part -> bytes
	keyOf   map[string]string
	singles []string // keys written with one PUT / CopyObject
	aborted int
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}, uploads: map[string]map[int][]byte{}, keyOf: map[string]string{}}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := strings.TrimPrefix(r.URL.Path, "/")
	q := r.URL.Query()
	body, _ := io.ReadAll(r.Body)
	switch {
	case r.Method == http.MethodHead:
		b, ok := f.objects[path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	case r.Method == http.MethodPost && q.Has("uploads"):
		id := "up" + strconv.Itoa(len(f.uploads)+1)
		f.uploads[id], f.keyOf[id] = map[int][]byte{}, path
		_, _ = w.Write([]byte("<InitiateMultipartUploadResult><UploadId>" + id + "</UploadId></InitiateMultipartUploadResult>"))
	case r.Method == http.MethodPut && q.Has("partNumber"):
		n, _ := strconv.Atoi(q.Get("partNumber"))
		data := body
		if src := r.Header.Get("X-Amz-Copy-Source"); src != "" {
			var a, b int
			rng := strings.TrimPrefix(r.Header.Get("X-Amz-Copy-Source-Range"), "bytes=")
			parts := strings.SplitN(rng, "-", 2)
			a, _ = strconv.Atoi(parts[0])
			b, _ = strconv.Atoi(parts[1])
			data = f.objects[strings.TrimPrefix(src, "/")][a : b+1]
			f.uploads[q.Get("uploadId")][n] = data
			_, _ = w.Write([]byte(`<CopyPartResult><ETag>"p` + strconv.Itoa(n) + `"</ETag></CopyPartResult>`))
			return
		}
		f.uploads[q.Get("uploadId")][n] = data
		w.Header().Set("ETag", `"p`+strconv.Itoa(n)+`"`)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		var c completeUpload
		if err := xml.Unmarshal(body, &c); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var all []byte
		for i, p := range c.Parts {
			if p.PartNumber != i+1 || p.ETag != `"p`+strconv.Itoa(i+1)+`"` {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			all = append(all, f.uploads[q.Get("uploadId")][p.PartNumber]...)
		}
		f.objects[f.keyOf[q.Get("uploadId")]] = all
		_, _ = w.Write([]byte("<CompleteMultipartUploadResult/>"))
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		f.aborted++
		delete(f.uploads, q.Get("uploadId"))
	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		f.objects[path] = f.objects[strings.TrimPrefix(r.Header.Get("X-Amz-Copy-Source"), "/")]
		f.singles = append(f.singles, path)
		_, _ = w.Write([]byte("<CopyObjectResult/>"))
	case r.Method == http.MethodPut:
		f.objects[path] = body
		f.singles = append(f.singles, path)
	default:
		w.WriteHeader(http.StatusNotImplemented)
	}
}

func testClient(t *testing.T, f *fakeS3) *Client {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c, err := NewClient(Config{Endpoint: strings.TrimPrefix(srv.URL, "http://"), URLStyle: "path", KeyID: "k", Secret: "s"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestMultipartPutAndCopy(t *testing.T) {
	t.Setenv(EnvMultipartThreshold, "1000")
	t.Setenv(EnvPartSize, "400")
	f := newFakeS3()
	c := testClient(t, f)
	ctx := context.Background()
	data := make([]byte, 1234) // 4 parts: 400, 400, 400, 34
	for i := range data {
		data[i] = byte(i * 7)
	}

	// A reader that is not an io.ReaderAt is buffered part by part.
	if err := c.Put(ctx, "b", "big.bin", io.MultiReader(bytes.NewReader(data)), int64(len(data)), "x"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.objects["b/big.bin"], data) || len(f.uploads) != 1 || len(f.singles) != 0 {
		t.Fatalf("multipart put: %d bytes, %d uploads, singles %v", len(f.objects["b/big.bin"]), len(f.uploads), f.singles)
	}
	// A ReaderAt (a file) is re-read for each part.
	if err := c.Put(ctx, "b", "big2.bin", bytes.NewReader(data), int64(len(data)), "x"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.objects["b/big2.bin"], data) {
		t.Fatal("multipart put of a ReaderAt differs")
	}
	// Below the threshold: one request.
	if err := c.Put(ctx, "b", "small.bin", bytes.NewReader(data[:10]), 10, "x"); err != nil || len(f.singles) != 1 {
		t.Fatalf("small put: %v singles=%v", err, f.singles)
	}

	// Copy: UploadPartCopy above the threshold (size known or asked with HEAD), CopyObject below.
	if err := c.CopySized(ctx, "b", "big.bin", "b", "copy.bin", int64(len(data))); err != nil || !bytes.Equal(f.objects["b/copy.bin"], data) {
		t.Fatalf("multipart copy: %v", err)
	}
	if err := c.CopySized(ctx, "b", "big.bin", "b", "copy2.bin", 0); err != nil || !bytes.Equal(f.objects["b/copy2.bin"], data) {
		t.Fatalf("multipart copy with HEAD: %v", err)
	}
	n := len(f.singles)
	if err := c.CopySized(ctx, "b", "small.bin", "b", "small2.bin", 10); err != nil || len(f.singles) != n+1 {
		t.Fatalf("small copy: %v", err)
	}
}

func TestPartSizeFor(t *testing.T) {
	if got := partSizeFor(1 << 30); got != DefaultPartSize {
		t.Errorf("default part size = %d", got)
	}
	// 5 TiB would need more than 10000 parts of 256 MiB.
	if got := partSizeFor(5 << 40); got*maxParts < 5<<40 {
		t.Errorf("part size %d cannot cover 5 TiB in %d parts", got, maxParts)
	}
}
