// Copyright 2018-2026 CERN
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

package editsession

import (
	"context"
	"errors"
	"time"

	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/rs/zerolog"
)

func (c *Coordinator) log(ctx context.Context, r ResourceID) zerolog.Logger {
	return appctx.GetLogger(ctx).With().Str("resource", r.Key()).Logger()
}

// sleep waits for d or until ctx is canceled. Returns false when canceled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func isNotFound(err error) bool {
	var nf notFound
	return errors.As(err, &nf)
}
