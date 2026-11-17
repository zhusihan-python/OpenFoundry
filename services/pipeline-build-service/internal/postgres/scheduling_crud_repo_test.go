package postgres

import (
	"testing"

	"github.com/google/uuid"

	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
)

// TestSchedulingActorMayManage pins the frozen authorization lattice:
// org = owner or admin; personal = owner ONLY (an admin claim never
// widens the personal domain); an unattributable owner (nil) leaves org
// admins as the only managers and blocks everyone in personal.
func TestSchedulingActorMayManage(t *testing.T) {
	owner := uuid.New()
	other := uuid.New()
	orgOwner := SchedulingActor{
		Tenant:  scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: uuid.New()},
		Subject: owner,
	}
	orgAdmin := SchedulingActor{
		Tenant:  scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: orgOwner.Tenant.ID},
		Subject: other, Admin: true,
	}
	orgMember := SchedulingActor{
		Tenant:  scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: orgOwner.Tenant.ID},
		Subject: other,
	}
	personalOwner := SchedulingActor{
		Tenant:  scheduleevents.TenantRef{Kind: scheduleevents.TenantKindPersonal, ID: owner},
		Subject: owner,
	}
	personalAdmin := SchedulingActor{
		Tenant:  scheduleevents.TenantRef{Kind: scheduleevents.TenantKindPersonal, ID: other},
		Subject: other, Admin: true,
	}

	cases := []struct {
		name  string
		actor SchedulingActor
		kind  scheduleevents.TenantKind
		owner *uuid.UUID
		want  bool
	}{
		{"org owner manages own", orgOwner, scheduleevents.TenantKindOrg, &owner, true},
		{"org member denied", orgMember, scheduleevents.TenantKindOrg, &owner, false},
		{"org admin override", orgAdmin, scheduleevents.TenantKindOrg, &owner, true},
		{"org unattributable owner admin-only", orgMember, scheduleevents.TenantKindOrg, nil, false},
		{"org unattributable owner admin passes", orgAdmin, scheduleevents.TenantKindOrg, nil, true},
		{"personal owner manages own", personalOwner, scheduleevents.TenantKindPersonal, &owner, true},
		{"personal admin never widens", personalAdmin, scheduleevents.TenantKindPersonal, &owner, false},
		{"personal unattributable owner denies all", personalOwner, scheduleevents.TenantKindPersonal, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.actor.MayManage(tc.kind, tc.owner); got != tc.want {
				t.Fatalf("MayManage=%v want %v", got, tc.want)
			}
		})
	}
}
