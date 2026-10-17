package s3conn

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

// Multipart limits. A single PUT or CopyObject reaches 5 GiB; beyond that S3
// needs a multipart upload (at most 10000 parts of 5 MiB to 5 GiB each).
const (
	// MaxSingleRequest is the largest object one PUT or CopyObject moves.
	MaxSingleRequest int64 = 5 << 30
	// DefaultPartSize is the part size of multipart transfers.
	DefaultPartSize int64 = 256 << 20
	minPartSize     int64 = 5 << 20
	maxParts              = 10000

	// EnvMultipartThreshold and EnvPartSize override the two limits above; they
	// exist so tests can exercise multipart with small objects (bytes; a part
	// smaller than 5 MiB is refused by real S3).
	EnvMultipartThreshold = "DUCT_S3_MULTIPART_THRESHOLD"
	EnvPartSize           = "DUCT_S3_PART_SIZE"
)

func envBytes(name string, def int64) int64 {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// multipartThreshold is the size above which Put and Copy use multipart.
func multipartThreshold() int64 { return envBytes(EnvMultipartThreshold, MaxSingleRequest) }

// partSizeFor returns the part size for an object of size bytes.
func partSizeFor(size int64) int64 {
	ps := envBytes(EnvPartSize, DefaultPartSize)
	if ps < minPartSize && os.Getenv(EnvPartSize) == "" {
		ps = minPartSize
	}
	if need := (size + maxParts - 1) / maxParts; need > ps {
		ps = need
	}
	return ps
}

type completedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeUpload struct {
	XMLName xml.Name        `xml:"CompleteMultipartUpload"`
	Parts   []completedPart `xml:"Part"`
}

func (c *Client) createMultipart(ctx context.Context, bucket, key string) (string, error) {
	q := url.Values{"uploads": {""}}
	resp, err := c.do(ctx, "create multipart "+key, http.MethodPost, c.endpointURL(bucket, key, q), nil, 0, emptyHash, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var r struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&r); err != nil || r.UploadID == "" {
		return "", fmt.Errorf("s3 create multipart %s: unreadable answer", key)
	}
	return r.UploadID, nil
}

func (c *Client) abortMultipart(bucket, key, uploadID string) {
	// Best effort, and not tied to the (possibly cancelled) caller context.
	q := url.Values{"uploadId": {uploadID}}
	resp, err := c.do(context.Background(), "abort multipart "+key, http.MethodDelete, c.endpointURL(bucket, key, q), nil, 0, emptyHash, nil)
	if err == nil {
		_ = resp.Body.Close()
	}
}

func (c *Client) completeMultipart(ctx context.Context, bucket, key, uploadID string, parts []completedPart) error {
	body, err := xml.Marshal(completeUpload{Parts: parts})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	q := url.Values{"uploadId": {uploadID}}
	resp, err := c.do(ctx, "complete multipart "+key, http.MethodPost, c.endpointURL(bucket, key, q), bytes.NewReader(body), int64(len(body)), hex.EncodeToString(sum[:]), nil)
	if err != nil {
		return err
	}
	// Like CopyObject, CompleteMultipartUpload can answer 200 with an error document.
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	if bytes.Contains(b, []byte("<Error>")) {
		return &StatusError{Op: "complete multipart " + key, Status: resp.StatusCode, Body: string(b)}
	}
	return nil
}

// putMultipart uploads size bytes of body in parts. Parts are sent one at a
// time; a body that is also an io.ReaderAt (a file) is re-read, anything else
// is buffered one part at a time.
func (c *Client) putMultipart(ctx context.Context, bucket, key string, body io.Reader, size int64) (err error) {
	uploadID, err := c.createMultipart(ctx, bucket, key)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			c.abortMultipart(bucket, key, uploadID)
		}
	}()
	ps := partSizeFor(size)
	ra, _ := body.(io.ReaderAt)
	var parts []completedPart
	for off, n := int64(0), 1; off < size; off, n = off+ps, n+1 {
		length := min(ps, size-off)
		var rd io.Reader
		var sum string
		if ra != nil {
			h := sha256.New()
			if _, err := io.Copy(h, io.NewSectionReader(ra, off, length)); err != nil {
				return err
			}
			sum, rd = hex.EncodeToString(h.Sum(nil)), io.NewSectionReader(ra, off, length)
		} else {
			buf := make([]byte, length)
			if _, err := io.ReadFull(body, buf); err != nil {
				return fmt.Errorf("s3 put %s: read part %d: %w", key, n, err)
			}
			h := sha256.Sum256(buf)
			sum, rd = hex.EncodeToString(h[:]), bytes.NewReader(buf)
		}
		q := url.Values{"partNumber": {strconv.Itoa(n)}, "uploadId": {uploadID}}
		resp, err := c.do(ctx, fmt.Sprintf("put %s part %d", key, n), http.MethodPut, c.endpointURL(bucket, key, q), rd, length, sum, nil)
		if err != nil {
			return err
		}
		etag := resp.Header.Get("ETag")
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		parts = append(parts, completedPart{PartNumber: n, ETag: etag})
	}
	return c.completeMultipart(ctx, bucket, key, uploadID, parts)
}

// copyMultipart copies an object of size bytes inside the server with
// UploadPartCopy (CopyObject is limited to 5 GiB).
func (c *Client) copyMultipart(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, size int64) (err error) {
	uploadID, err := c.createMultipart(ctx, dstBucket, dstKey)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			c.abortMultipart(dstBucket, dstKey, uploadID)
		}
	}()
	src := "/" + srcBucket + "/" + escapePath(srcKey)
	ps := partSizeFor(size)
	var parts []completedPart
	for off, n := int64(0), 1; off < size; off, n = off+ps, n+1 {
		last := min(off+ps, size) - 1
		q := url.Values{"partNumber": {strconv.Itoa(n)}, "uploadId": {uploadID}}
		resp, err := c.do(ctx, fmt.Sprintf("copy %s part %d", srcKey, n), http.MethodPut, c.endpointURL(dstBucket, dstKey, q), nil, 0, emptyHash,
			map[string]string{"X-Amz-Copy-Source": src, "X-Amz-Copy-Source-Range": fmt.Sprintf("bytes=%d-%d", off, last)})
		if err != nil {
			return err
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		if bytes.Contains(b, []byte("<Error>")) {
			return &StatusError{Op: fmt.Sprintf("copy %s part %d", srcKey, n), Status: resp.StatusCode, Body: string(b)}
		}
		var r struct {
			ETag string `xml:"ETag"`
		}
		if err := xml.Unmarshal(b, &r); err != nil || r.ETag == "" {
			return errors.New("s3 copy " + srcKey + ": unreadable part answer")
		}
		parts = append(parts, completedPart{PartNumber: n, ETag: r.ETag})
	}
	return c.completeMultipart(ctx, dstBucket, dstKey, uploadID, parts)
}

// CopySized copies an object inside the server. size is the object's length if
// known (<= 0: it is asked with a HEAD only when multipart could be needed,
// which is always, so pass it when you have it). Objects above the single
// request limit are copied with UploadPartCopy.
func (c *Client) CopySized(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, size int64) error {
	if size <= 0 {
		o, err := c.Head(ctx, srcBucket, srcKey)
		if err != nil {
			return err
		}
		size = o.Size
	}
	if size > multipartThreshold() {
		return c.copyMultipart(ctx, srcBucket, srcKey, dstBucket, dstKey, size)
	}
	return c.Copy(ctx, srcBucket, srcKey, dstBucket, dstKey)
}
