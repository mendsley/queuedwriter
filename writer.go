/*
 * Copyright 2014 Matthew Endsley
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted providing that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package queuedwriter

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

var ErrTooLarge = errors.New("queuedWriter.W: too large")

type flusher interface {
	Flush() error
}

type W struct {
	w        io.Writer
	f        flusher
	out      *bytes.Buffer
	cond     sync.Cond
	wg       sync.WaitGroup
	lock     sync.Mutex
	closed   bool
	flushReq bool
	maxSize  int64
	cb       []func(w io.Writer) error
	err      error
}

// Create a new queued writer
func New(w io.Writer) *W {
	return NewSize(w, 0x7fffffffffffffff)
}

// Craete a new queued writer with a max size
func NewSize(w io.Writer, maxSize int64) *W {
	qw := &W{
		w:       w,
		out:     new(bytes.Buffer),
		maxSize: maxSize,
	}

	if f, ok := w.(flusher); ok {
		qw.f = f
	}

	qw.cond.L = &qw.lock

	qw.wg.Add(1)
	go qw.proc()
	return qw
}

// Wait for background process to exit
func (w *W) Wait() {
	w.wg.Wait()
}

func (w *W) Close() {
	w.lock.Lock()
	w.closed = true
	w.lock.Unlock()

	w.cond.Signal()
}

// Flush the underlying stream if supported
func (w *W) Flush() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.err != nil {
		return w.err
	}

	// only request the flush if supported
	if w.f != nil {
		w.flushReq = true
		w.cond.Signal()
	}

	return nil
}

// Push a callback function to run an external process
// on the writer
func (w *W) PushCallback(fn func(io.Writer) error) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.err != nil {
		return w.err
	}

	w.cb = append(w.cb, fn)
	w.cond.Signal()
	return nil
}

// Write data to the queue
func (w *W) Write(p []byte) (int, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if int64(w.out.Len())+int64(len(p)) > w.maxSize {
		w.err = ErrTooLarge
	}

	if w.err != nil {
		return 0, w.err
	}

	n, err := w.out.Write(p)
	w.cond.Signal()
	return n, err
}

// Write a string to the queue
func (w *W) WriteString(s string) (int, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if int64(w.out.Len())+int64(len(s)) > w.maxSize {
		w.err = ErrTooLarge
	}

	if w.err != nil {
		return 0, w.err
	}

	n, err := w.out.WriteString(s)
	w.cond.Signal()
	return n, err
}

// Copy an io.Reader to the queue
func (w *W) ReadFrom(r io.Reader) (int64, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	var total int64

	// copy in blocks, unlocking the mutex
	// to allow the background proc to flush data out
	for {
		if w.err != nil {
			return total, w.err
		}

		n, err := io.CopyN(w.out, r, 4096)
		total += n
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return total, err
		}

		if int64(w.out.Len()) > w.maxSize {
			w.err = ErrTooLarge
		}

		// pulse the lock to allow the background proc
		// a chance to flush data
		w.cond.Signal()
	}
}

// background proc to write data to the underlying io.Writer
func (w *W) proc() {
	back := new(bytes.Buffer)
	var (
		callbacks []func(io.Writer) error
		flushReq  bool
	)

	w.lock.Lock()
	defer func() {
		w.lock.Unlock()
		w.wg.Done()
	}()

	for {

		// wait for something to do
		for !w.closed && w.out.Len() == 0 && !w.flushReq && len(w.cb) == 0 {
			w.cond.Wait()
		}

		if w.closed {
			return
		}

		// swap buffers; no need to hold lock during I/O
		back.Reset()
		w.out, back = back, w.out
		callbacks, w.cb = w.cb, callbacks[:0]
		flushReq, w.flushReq = w.flushReq, false

		// BEGIN --- unlocked for I/O ---
		w.lock.Unlock()
		var err error
		if back.Len() != 0 {
			_, err = w.w.Write(back.Bytes())
		}
		if err == nil {
			// run callbacks
			for _, fn := range callbacks {
				err = fn(w.w)
				if err != nil {
					break
				}
			}
		}
		if err == nil && flushReq {
			err = w.f.Flush()
		}
		w.lock.Lock()
		// END   --- unlocked for I/O ---

		if err != nil {
			w.err = err
			return
		}
	}
}
