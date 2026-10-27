package bond

import "testing"

func TestBondMemberSnapshotKeepsServingOwnership(t *testing.T) {
	h, _, _ := bondFixture(t)
	members := h.Inbounds()
	if len(members) != 1 || members[0].Tag() != "leg" || members[0] != h.inbounds[0] {
		t.Fatal("snapshot did not expose the actual serving member")
	}
	members[0] = nil
	if h.Inbounds()[0] == nil {
		t.Fatal("inspection changed the serving member list")
	}
}
