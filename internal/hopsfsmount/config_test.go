package hopsfsmount

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListingAttrsDuration(t *testing.T) {
	for _, secs := range []int{60, 5, 0} {
		t.Run(fmt.Sprintf("%d accepted", secs), func(t *testing.T) {
			d, err := listingAttrsDuration(secs)
			assert.Nil(t, err)
			assert.Equal(t, time.Duration(secs)*time.Second, d)
		})
	}
	t.Run("negative rejected", func(t *testing.T) {
		_, err := listingAttrsDuration(-1)
		assert.EqualError(t, err, "cacheListingAttrsSecs can not be negative")
	})
}
