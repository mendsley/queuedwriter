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
	"io"
	"sync"
)

type W struct {
	W       io.Writer
	out     *bytes.Buffer
	wg      sync.WaitGroup
	lock    sync.Mutex
	err     error
	running bool
}

// Create a new queued writer
func New(w io.Writer) *W {
	qw := &W{
		W:   w,
		out: new(bytes.Buffer),
	}
	return qw
}

// Wait for background process to exit
func (w *W) Wait() {
	w.wg.Wait()
}

// Write data to the queue
func (w *W) Write(p []byte) (int, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.err != nil {
		return 0, w.err
	}

	if !w.running {
		w.wg.Add(1)
		w.running = true
		go w.proc()
	}

	return w.out.Write(p)
}

// Write a string to the queue
func (w *W) WriteString(s string) (int, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.err != nil {
		return 0, w.err
	}

	if !w.running {
		w.wg.Add(1)
		w.running = true
		go w.proc()
	}

	return w.out.WriteString(s)
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

		if !w.running {
			w.wg.Add(1)
			w.running = true
			go w.proc()
		}

		n, err := io.CopyN(w.out, r, 4096)
		total += n
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return total, err
		}

		// pulse the lock to allow the background proc
		// a chance to flush data
		w.lock.Unlock()
		w.lock.Lock()
	}
}

// background proc to write data to the underlying io.Writer
func (w *W) proc() {
	back := new(bytes.Buffer)

	w.lock.Lock()
	defer func() {
		w.running = false
		w.lock.Unlock()
		w.wg.Done()
	}()

	for {
		if w.out.Len() == 0 {
			return
		}

		// swap buffers; no need to hold lock during I/O
		back.Reset()
		w.out, back = back, w.out

		var err error
		if back.Len() != 0 {
			// BEGIN --- unlocked for I/O ---
			w.lock.Unlock()
			_, err = w.W.Write(back.Bytes())
			w.lock.Lock()
			// END   --- unlocked for I/O ---

			if err != nil {
				w.err = err
				return
			}
		}
	}
}
