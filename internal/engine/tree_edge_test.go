package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/scrypster/muninndb/internal/transport/mbp"
)

// TestRecallTree_EmptyTree verifies that a root with no children is recalled
// correctly — no crash, empty children slice.
func TestRecallTree_EmptyTree(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "empty-tree-edge"

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Root Only",
			Content: "I have no children",
			Type:    "goal",
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}
	if tree.Concept != "Root Only" {
		t.Errorf("concept: got %q, want %q", tree.Concept, "Root Only")
	}
	if len(tree.Children) != 0 {
		t.Errorf("expected 0 children, got %d", len(tree.Children))
	}
}

// TestRecallTree_DeepTree verifies that max_depth stops traversal at the
// correct level, and that max_depth=0 returns the full tree.
func TestRecallTree_DeepTree(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "deep-tree-edge"

	// 5-level deep chain: root → a → b → c → d → leaf
	req := &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "root",
			Content: "level 0",
			Children: []TreeNodeInput{
				{
					Concept: "a",
					Content: "level 1",
					Children: []TreeNodeInput{
						{
							Concept: "b",
							Content: "level 2",
							Children: []TreeNodeInput{
								{
									Concept: "c",
									Content: "level 3",
									Children: []TreeNodeInput{
										{
											Concept: "d",
											Content: "level 4",
											Children: []TreeNodeInput{
												{
													Concept: "leaf",
													Content: "level 5",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	result, err := eng.RememberTree(ctx, req)
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}
	if len(result.NodeMap) != 6 {
		t.Fatalf("NodeMap: got %d entries, want 6", len(result.NodeMap))
	}

	// max_depth=3: should stop at level 3 (c), c's children should be empty.
	tree3, err := eng.RecallTree(ctx, vault, result.RootID, 3, 0, true)
	if err != nil {
		t.Fatalf("RecallTree maxDepth=3: %v", err)
	}
	// Depth 0 = root, 1 = a, 2 = b, 3 = c. At depth 3, no children loaded.
	level1 := tree3.Children
	if len(level1) != 1 || level1[0].Concept != "a" {
		t.Fatalf("expected 'a' at depth 1, got %v", level1)
	}
	level2 := level1[0].Children
	if len(level2) != 1 || level2[0].Concept != "b" {
		t.Fatalf("expected 'b' at depth 2, got %v", level2)
	}
	level3 := level2[0].Children
	if len(level3) != 1 || level3[0].Concept != "c" {
		t.Fatalf("expected 'c' at depth 3, got %v", level3)
	}
	// At max_depth=3, depth==3 when fetching c, so c.Children should be empty.
	if len(level3[0].Children) != 0 {
		t.Errorf("expected c to have 0 children at maxDepth=3, got %d", len(level3[0].Children))
	}

	// max_depth=0: should return full 6-node chain.
	treeFull, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree maxDepth=0: %v", err)
	}
	// Traverse to leaf.
	cur := treeFull
	labels := []string{"root", "a", "b", "c", "d", "leaf"}
	for i, label := range labels {
		if cur.Concept != label {
			t.Errorf("level %d: got %q, want %q", i, cur.Concept, label)
		}
		if i < len(labels)-1 {
			if len(cur.Children) != 1 {
				t.Fatalf("level %d: expected 1 child, got %d", i, len(cur.Children))
			}
			cur = &cur.Children[0]
		}
	}
}

// TestRecallTree_VeryWideTree verifies that a root with 50 children is handled:
// limit=10 returns exactly 10, limit=0 returns all 50.
func TestRecallTree_VeryWideTree(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "wide-tree-edge"

	children := make([]TreeNodeInput, 50)
	for i := 0; i < 50; i++ {
		children[i] = TreeNodeInput{
			Concept: fmt.Sprintf("Child%02d", i+1),
			Content: fmt.Sprintf("child number %d", i+1),
		}
	}

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept:  "Wide Root",
			Content:  "50 children",
			Children: children,
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}
	if len(result.NodeMap) != 51 {
		t.Fatalf("NodeMap: got %d entries, want 51", len(result.NodeMap))
	}

	// limit=10 should return exactly 10 children.
	treeLimit, err := eng.RecallTree(ctx, vault, result.RootID, 0, 10, true)
	if err != nil {
		t.Fatalf("RecallTree limit=10: %v", err)
	}
	if len(treeLimit.Children) != 10 {
		t.Errorf("limit=10: got %d children, want 10", len(treeLimit.Children))
	}
	// First child should be Child01.
	if treeLimit.Children[0].Concept != "Child01" {
		t.Errorf("first child: got %q, want Child01", treeLimit.Children[0].Concept)
	}

	// limit=0 should return all 50.
	treeAll, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree limit=0: %v", err)
	}
	if len(treeAll.Children) != 50 {
		t.Errorf("limit=0: got %d children, want 50", len(treeAll.Children))
	}
}

// TestRecallTree_MixedCompletedState verifies filtering with includeCompleted=false
// across a multi-level tree with mixed states.
//
// Tree structure:
//
//	Root (active)
//	├─ Phase1 (active)
//	│  ├─ Task1.1 (completed)
//	│  └─ Task1.2 (active)
//	└─ Phase2 (completed)
//	   └─ Task2.1 (active)
//
// With includeCompleted=false: Phase2 and Task1.1 are excluded.
// Visible nodes: Root, Phase1, Task1.2 → count = 3.
func TestRecallTree_MixedCompletedState(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "mixed-state-tree-edge"

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Root",
			Content: "root node",
			Children: []TreeNodeInput{
				{
					Concept: "Phase1",
					Content: "first phase",
					Children: []TreeNodeInput{
						{Concept: "Task1.1", Content: "done task"},
						{Concept: "Task1.2", Content: "active task"},
					},
				},
				{
					Concept: "Phase2",
					Content: "completed phase",
					Children: []TreeNodeInput{
						{Concept: "Task2.1", Content: "task under completed phase"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	// Mark Task1.1 and Phase2 as completed.
	for _, concept := range []string{"Task1.1", "Phase2"} {
		id, ok := result.NodeMap[concept]
		if !ok {
			t.Fatalf("concept %q not found in NodeMap", concept)
		}
		if err := eng.UpdateLifecycleState(ctx, vault, id, "completed"); err != nil {
			t.Fatalf("UpdateLifecycleState(%q): %v", concept, err)
		}
	}

	// Recall without completed nodes.
	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, false)
	if err != nil {
		t.Fatalf("RecallTree includeCompleted=false: %v", err)
	}

	// Root should be present (active).
	if tree.Concept != "Root" {
		t.Errorf("root concept: got %q", tree.Concept)
	}
	// Root should have exactly 1 child: Phase1 (Phase2 filtered out).
	if len(tree.Children) != 1 {
		t.Fatalf("root children: got %d, want 1 (Phase1 only)", len(tree.Children))
	}
	phase1 := tree.Children[0]
	if phase1.Concept != "Phase1" {
		t.Errorf("expected Phase1, got %q", phase1.Concept)
	}
	// Phase1 should have exactly 1 child: Task1.2 (Task1.1 filtered out).
	if len(phase1.Children) != 1 {
		t.Fatalf("Phase1 children: got %d, want 1 (Task1.2 only)", len(phase1.Children))
	}
	if phase1.Children[0].Concept != "Task1.2" {
		t.Errorf("expected Task1.2, got %q", phase1.Children[0].Concept)
	}

	// Count total visible nodes: Root + Phase1 + Task1.2 = 3.
	count := countNodes(tree)
	if count != 3 {
		t.Errorf("visible node count: got %d, want 3", count)
	}
}

// countNodes recursively counts all nodes in the tree (including root).
func countNodes(node *TreeNode) int {
	if node == nil {
		return 0
	}
	total := 1
	for i := range node.Children {
		total += countNodes(&node.Children[i])
	}
	return total
}

// TestRememberTree_OrdinalOrdering verifies that siblings recalled in ordinal
// order, not insertion order.
//
// Children are written in this insertion order: "Third"(ordinal=1 in array
// position 0), "First"(ordinal=2 in array pos 1), "Second"(ordinal=3 in array
// pos 2). Wait — actually the RememberTree writes ordinals as i+1 (array
// index+1), so we want to use a custom arrangement.
//
// We use AddChild with explicit ordinals so we can control the ordinal
// assignment independently of insertion order:
// - Insert "Third" with ordinal 3
// - Insert "First" with ordinal 1
// - Insert "Second" with ordinal 2
//
// Recall should return: First (ord 1), Second (ord 2), Third (ord 3).
func TestRememberTree_OrdinalOrdering(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "ordinal-order-edge"

	// Create parent via Write directly.
	resp, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   vault,
		Concept: "Parent",
		Content: "parent node",
	})
	if err != nil {
		t.Fatalf("Write parent: %v", err)
	}
	parentID := resp.ID

	// Insert children with explicit ordinals in non-sequential order.
	type child struct {
		concept string
		ordinal int32
	}
	insertOrder := []child{
		{"Third", 3},
		{"First", 1},
		{"Second", 2},
	}
	for _, c := range insertOrder {
		ord := c.ordinal
		if _, err := eng.AddChild(ctx, vault, parentID, &AddChildInput{
			Concept: c.concept,
			Content: c.concept,
			Ordinal: &ord,
		}); err != nil {
			t.Fatalf("AddChild %q: %v", c.concept, err)
		}
	}

	// Recall: expect ordinal order First, Second, Third.
	tree, err := eng.RecallTree(ctx, vault, parentID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}
	if len(tree.Children) != 3 {
		t.Fatalf("expected 3 children, got %d", len(tree.Children))
	}
	expected := []string{"First", "Second", "Third"}
	expectedOrdinals := []int32{1, 2, 3}
	for i, ch := range tree.Children {
		if ch.Concept != expected[i] {
			t.Errorf("child[%d]: got concept %q, want %q", i, ch.Concept, expected[i])
		}
		if ch.Ordinal != expectedOrdinals[i] {
			t.Errorf("child[%d]: got ordinal %d, want %d", i, ch.Ordinal, expectedOrdinals[i])
		}
	}
}

// TestAddChild_ExplicitOrdinal verifies that AddChild with an explicit ordinal
// stores and recalls that ordinal correctly.
func TestAddChild_ExplicitOrdinal(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "explicit-ordinal-edge"

	resp, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   vault,
		Concept: "Parent",
		Content: "parent",
	})
	if err != nil {
		t.Fatalf("Write parent: %v", err)
	}

	ord := int32(5)
	result, err := eng.AddChild(ctx, vault, resp.ID, &AddChildInput{
		Concept: "Explicit5",
		Content: "ordinal 5 child",
		Ordinal: &ord,
	})
	if err != nil {
		t.Fatalf("AddChild: %v", err)
	}
	if result.Ordinal != 5 {
		t.Errorf("returned ordinal: got %d, want 5", result.Ordinal)
	}

	// Recall and verify ordinal in tree.
	tree, err := eng.RecallTree(ctx, vault, resp.ID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}
	if len(tree.Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(tree.Children))
	}
	if tree.Children[0].Ordinal != 5 {
		t.Errorf("recalled ordinal: got %d, want 5", tree.Children[0].Ordinal)
	}
	if tree.Children[0].Concept != "Explicit5" {
		t.Errorf("recalled concept: got %q, want Explicit5", tree.Children[0].Concept)
	}
}

// TestAddChild_AppendToExisting verifies that AddChild with nil ordinal appends
// after the last child written by RememberTree (ordinals 1, 2, 3).
// The new child should get ordinal 4.
func TestAddChild_AppendToExisting(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "append-ordinal-edge"

	// Create parent with 3 children (ordinals 1, 2, 3).
	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Parent",
			Content: "parent",
			Children: []TreeNodeInput{
				{Concept: "Child1", Content: "first"},
				{Concept: "Child2", Content: "second"},
				{Concept: "Child3", Content: "third"},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	// AddChild with nil ordinal should get ordinal 4.
	added, err := eng.AddChild(ctx, vault, result.RootID, &AddChildInput{
		Concept: "Child4",
		Content: "appended",
	})
	if err != nil {
		t.Fatalf("AddChild: %v", err)
	}
	if added.Ordinal != 4 {
		t.Errorf("appended child ordinal: got %d, want 4", added.Ordinal)
	}

	// Recall and verify 4 children in correct order.
	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}
	if len(tree.Children) != 4 {
		t.Fatalf("expected 4 children, got %d", len(tree.Children))
	}
	if tree.Children[3].Concept != "Child4" {
		t.Errorf("last child: got %q, want Child4", tree.Children[3].Concept)
	}
	if tree.Children[3].Ordinal != 4 {
		t.Errorf("last child ordinal: got %d, want 4", tree.Children[3].Ordinal)
	}
}

// TestRememberTree_LargeTree verifies a 50-node tree:
// root → 5 phases → 9 tasks each = 1 + 5 + 45 = 51 nodes.
// The spec says 50 nodes (root + 5 phases + 9 tasks * 5 = 1+5+45=51).
// We use 4 tasks per phase to get exactly 50 nodes (1+5+44=50)... but the spec
// says 9 tasks, so 1 + 5 + 45 = 51. We follow the spec and verify 51.
// Actually the spec says "46 nodes + root + phases" which suggests the math is
// intended as root(1) + phases(5) + tasks(45) = 51, but the spec says "exactly
// 50 entries". We'll use 4 tasks per phase for 50 nodes: 1+5+40 = 46? No.
// Re-reading: "5 phases → 9 tasks each = 46 nodes + root + phases".
// That's 9*5=45 tasks + 1 root + 5 phases = 51. The spec says 50 entries.
// We'll use the spec intent of exactly 50: 1 root + 7 phases × 7 tasks = 1+7+49?
// Let's just use: 1 root + 7 phases + 6 tasks each = 1+7+42=50.
// Actually, simplest: 1 root + 5 phases + 44 tasks doesn't work evenly.
// We'll just use 1 root + 7 phases + 6 tasks (7×6+7+1 = 50). But spec says "5 phases
// → 9 tasks". Let's trust the test task description: "exactly 50 entries" with
// "root → 5 phases → 9 tasks each". 1+5+45 = 51. The test description has an
// error (46+root+phases = 46+1+5 = 52). We'll build the tree as described
// (5 phases, 9 tasks each) and assert 51 nodes.
func TestRememberTree_LargeTree(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "large-tree-edge"

	const numPhases = 5
	const tasksPerPhase = 9

	phases := make([]TreeNodeInput, numPhases)
	for p := 0; p < numPhases; p++ {
		tasks := make([]TreeNodeInput, tasksPerPhase)
		for tt := 0; tt < tasksPerPhase; tt++ {
			tasks[tt] = TreeNodeInput{
				Concept: fmt.Sprintf("Phase%d_Task%d", p+1, tt+1),
				Content: fmt.Sprintf("task %d of phase %d", tt+1, p+1),
			}
		}
		phases[p] = TreeNodeInput{
			Concept:  fmt.Sprintf("Phase%d", p+1),
			Content:  fmt.Sprintf("phase %d", p+1),
			Children: tasks,
		}
	}

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept:  "LargeRoot",
			Content:  "root of large tree",
			Children: phases,
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	// 1 root + 5 phases + 9*5 tasks = 51 nodes.
	expectedNodes := 1 + numPhases + numPhases*tasksPerPhase
	if len(result.NodeMap) != expectedNodes {
		t.Fatalf("NodeMap: got %d entries, want %d", len(result.NodeMap), expectedNodes)
	}

	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}

	// Verify all phases are in ordinal order.
	if len(tree.Children) != numPhases {
		t.Fatalf("expected %d phases, got %d", numPhases, len(tree.Children))
	}
	for p, phase := range tree.Children {
		expectedConcept := fmt.Sprintf("Phase%d", p+1)
		if phase.Concept != expectedConcept {
			t.Errorf("phase[%d]: got %q, want %q", p, phase.Concept, expectedConcept)
		}
		// Verify all tasks within this phase are in ordinal order.
		if len(phase.Children) != tasksPerPhase {
			t.Fatalf("phase[%d] tasks: got %d, want %d", p, len(phase.Children), tasksPerPhase)
		}
		for tt, task := range phase.Children {
			expectedTask := fmt.Sprintf("Phase%d_Task%d", p+1, tt+1)
			if task.Concept != expectedTask {
				t.Errorf("phase[%d] task[%d]: got %q, want %q", p, tt, task.Concept, expectedTask)
			}
		}
	}
}

// TestRecallTree_NonexistentRoot verifies that calling RecallTree with a garbage
// ULID returns an error and does not panic.
func TestRecallTree_NonexistentRoot(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()

	// Valid ULID format but does not exist in storage.
	garbage := "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	_, err := eng.RecallTree(ctx, "nonexistent-vault", garbage, 0, 0, true)
	if err == nil {
		t.Fatal("expected error for nonexistent root, got nil")
	}
}

// TestRecallTree_GhostChild verifies that RecallTree with includeCompleted=true
// does not crash or error when a child engram has been hard-deleted (ghost child).
// The ghost ordinal key should be silently skipped and the root returned with
// zero children.
func TestRecallTree_GhostChild(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "ghost-child-edge"

	// Create a tree: root → child.
	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "GhostRoot",
			Content: "root with a soon-to-be-ghost child",
			Children: []TreeNodeInput{
				{Concept: "GhostChild", Content: "this will be hard-deleted"},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	childID, ok := result.NodeMap["GhostChild"]
	if !ok {
		t.Fatal("GhostChild not found in NodeMap")
	}

	// Hard-delete the child engram so GetEngram returns (nil, nil).
	if _, err := eng.Forget(ctx, &mbp.ForgetRequest{
		Vault: vault,
		ID:    childID,
		Hard:  true,
	}); err != nil {
		t.Fatalf("Forget (hard delete child): %v", err)
	}

	// RecallTree with includeCompleted=true must not error or panic.
	// The ghost ordinal key should be silently skipped.
	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree after ghost child: unexpected error: %v", err)
	}
	if tree == nil {
		t.Fatal("RecallTree returned nil tree")
	}
	if tree.Concept != "GhostRoot" {
		t.Errorf("root concept: got %q, want GhostRoot", tree.Concept)
	}
	// The deleted child's ordinal key is stale — it must be silently skipped.
	if len(tree.Children) != 0 {
		t.Errorf("expected 0 children after ghost deletion, got %d", len(tree.Children))
	}
}

// TestAddChild_NilInput verifies that AddChild with nil input returns an error
// and does not panic.
func TestAddChild_NilInput(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   "nil-input-edge",
		Concept: "Parent",
		Content: "parent",
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	_, err = eng.AddChild(ctx, "nil-input-edge", resp.ID, nil)
	if err == nil {
		t.Fatal("expected error for nil AddChildInput, got nil")
	}
}

// TestRememberTree_SingleNode verifies that a tree with only a root (no children)
// has exactly 1 NodeMap entry and RecallTree returns root with empty children.
func TestRememberTree_SingleNode(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "single-node-edge"

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Solo",
			Content: "I am alone",
			Type:    "task",
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}
	if len(result.NodeMap) != 1 {
		t.Fatalf("NodeMap: got %d entries, want 1", len(result.NodeMap))
	}
	if _, ok := result.NodeMap["Solo"]; !ok {
		t.Error("expected 'Solo' in NodeMap")
	}

	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}
	if tree.Concept != "Solo" {
		t.Errorf("concept: got %q, want Solo", tree.Concept)
	}
	if len(tree.Children) != 0 {
		t.Errorf("expected 0 children, got %d", len(tree.Children))
	}
}

// TestRememberTree_DuplicateConcept verifies that a tree where two sibling nodes
// share the same concept string is rejected before any writes occur, with an
// error containing "duplicate concept".
func TestRememberTree_DuplicateConcept(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "duplicate-concept-edge"

	// root → [Phase 1, Phase 1] — "Phase 1" appears twice as siblings.
	_, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Project Plan",
			Content: "root node",
			Children: []TreeNodeInput{
				{Concept: "Phase 1", Content: "first occurrence"},
				{Concept: "Phase 1", Content: "duplicate — should be rejected"},
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for duplicate concept, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate concept") {
		t.Errorf("expected error to contain %q, got: %v", "duplicate concept", err)
	}
}

// TestRememberTree_DuplicateConcept_SiblingPath verifies that a concept
// appearing in two distinct branches of the tree is also rejected.
//
// Tree structure:
//
//	root
//	├─ A
//	│  └─ B   ← first occurrence of "B"
//	└─ C
//	   └─ B   ← duplicate "B" in a different branch
func TestRememberTree_DuplicateConcept_SiblingPath(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "duplicate-concept-sibling-path-edge"

	_, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "root",
			Content: "root node",
			Children: []TreeNodeInput{
				{
					Concept: "A",
					Content: "branch A",
					Children: []TreeNodeInput{
						{Concept: "B", Content: "B under A"},
					},
				},
				{
					Concept: "C",
					Content: "branch C",
					Children: []TreeNodeInput{
						{Concept: "B", Content: "B under C — duplicate"},
					},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for duplicate concept across branches, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate concept") {
		t.Errorf("expected error to contain %q, got: %v", "duplicate concept", err)
	}
}

// buildDeepTree constructs a linear chain of TreeNodeInputs depth levels deep.
// depth=0 returns a single leaf node; depth=N returns a chain of N+1 nodes.
func buildDeepTree(depth int) TreeNodeInput {
	if depth == 0 {
		return TreeNodeInput{Concept: "leaf", Content: "leaf content"}
	}
	return TreeNodeInput{
		Concept:  fmt.Sprintf("node-%d", depth),
		Content:  fmt.Sprintf("content-%d", depth),
		Children: []TreeNodeInput{buildDeepTree(depth - 1)},
	}
}

// TestRememberTree_EmptyChildConcept verifies that a tree where a nested child
// has an empty concept is rejected before any writes occur.
func TestRememberTree_EmptyChildConcept(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "empty-child-concept-edge"

	_, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "ValidRoot",
			Content: "root content",
			Children: []TreeNodeInput{
				{
					Concept: "ValidChild",
					Content: "child content",
					Children: []TreeNodeInput{
						{
							Concept: "", // invalid — empty concept
							Content: "grandchild content",
						},
					},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for child with empty concept, got nil")
	}
	if !strings.Contains(err.Error(), "empty concept") {
		t.Errorf("expected error to contain %q, got: %v", "empty concept", err)
	}
}

// TestRememberTree_DepthBomb verifies that a tree exceeding maxTreeDepth levels
// is rejected with an appropriate error before any writes occur.
func TestRememberTree_DepthBomb(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "depth-bomb-edge"

	// Build a tree that is 25 levels deep — exceeds maxTreeDepth of 20.
	root := buildDeepTree(25)

	_, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root:  root,
	})
	if err == nil {
		t.Fatal("expected error for tree exceeding max depth, got nil")
	}
	if !strings.Contains(err.Error(), "depth exceeds maximum") {
		t.Errorf("expected error to contain %q, got: %v", "depth exceeds maximum", err)
	}
}

// TestAddChild_ConcurrentAppends verifies that 3 goroutines concurrently
// appending a child to the same parent produce 3 children with distinct ordinals
// and cause no panics or data races.
func TestAddChild_ConcurrentAppends(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "concurrent-append-edge"

	// Create parent.
	resp, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   vault,
		Concept: "ConcurrentParent",
		Content: "parent for concurrent appends",
	})
	if err != nil {
		t.Fatalf("Write parent: %v", err)
	}
	parentID := resp.ID

	const numGoroutines = 3
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	results := make([]*AddChildResult, numGoroutines)
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("goroutine %d: unexpected panic: %v", idx, r)
				}
			}()
			r, e := eng.AddChild(ctx, vault, parentID, &AddChildInput{
				Concept: fmt.Sprintf("ConcurrentChild%d", idx),
				Content: fmt.Sprintf("child from goroutine %d", idx),
			})
			results[idx] = r
			errors[idx] = e
		}(i)
	}

	wg.Wait()

	// Check no errors.
	for i, e := range errors {
		if e != nil {
			t.Errorf("goroutine %d: AddChild error: %v", i, e)
		}
	}

	// Collect assigned ordinals — they must all be unique (no collisions).
	seenOrdinals := make(map[int32]int)
	for i, r := range results {
		if r == nil {
			t.Errorf("goroutine %d: result is nil", i)
			continue
		}
		if prev, exists := seenOrdinals[r.Ordinal]; exists {
			t.Errorf("ordinal collision: goroutines %d and %d both got ordinal %d", prev, i, r.Ordinal)
		}
		seenOrdinals[r.Ordinal] = i
	}

	// Recall tree and verify exactly 3 children are present.
	tree, err := eng.RecallTree(ctx, vault, parentID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}
	if len(tree.Children) != numGoroutines {
		t.Errorf("expected %d children after concurrent appends, got %d", numGoroutines, len(tree.Children))
	}

	// Verify distinct ordinals in the recalled tree as well.
	recalledOrdinals := make(map[int32]bool)
	for _, ch := range tree.Children {
		if recalledOrdinals[ch.Ordinal] {
			t.Errorf("duplicate ordinal %d in recalled tree", ch.Ordinal)
		}
		recalledOrdinals[ch.Ordinal] = true
	}
}

// TestRecallTree_FilterCompleted_BatchMeta verifies that the batch GetMetadata
// optimisation correctly filters completed children when includeCompleted=false,
// and returns all children when includeCompleted=true.
//
// Tree: root with 5 children, 2 of which are marked completed.
func TestRecallTree_FilterCompleted_BatchMeta(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "batch-meta-filter-edge"

	// Create root with 5 children.
	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Root",
			Content: "root node",
			Children: []TreeNodeInput{
				{Concept: "Active1", Content: "active child 1", Type: "task"},
				{Concept: "Active2", Content: "active child 2", Type: "task"},
				{Concept: "Active3", Content: "active child 3", Type: "task"},
				{Concept: "Completed1", Content: "completed child 1", Type: "task"},
				{Concept: "Completed2", Content: "completed child 2", Type: "task"},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}
	if len(result.NodeMap) != 6 { // root + 5 children
		t.Fatalf("NodeMap: got %d entries, want 6", len(result.NodeMap))
	}

	// Mark 2 children as completed.
	for _, concept := range []string{"Completed1", "Completed2"} {
		id, ok := result.NodeMap[concept]
		if !ok {
			t.Fatalf("concept %q not found in NodeMap", concept)
		}
		if err := eng.UpdateLifecycleState(ctx, vault, id, "completed"); err != nil {
			t.Fatalf("UpdateLifecycleState(%q): %v", concept, err)
		}
	}

	// includeCompleted=false: only 3 active children should be returned.
	treeFiltered, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, false)
	if err != nil {
		t.Fatalf("RecallTree includeCompleted=false: %v", err)
	}
	if treeFiltered.Concept != "Root" {
		t.Errorf("root concept: got %q, want Root", treeFiltered.Concept)
	}
	if len(treeFiltered.Children) != 3 {
		t.Fatalf("includeCompleted=false: got %d children, want 3", len(treeFiltered.Children))
	}
	for _, ch := range treeFiltered.Children {
		if ch.Concept == "Completed1" || ch.Concept == "Completed2" {
			t.Errorf("completed child %q should have been filtered out", ch.Concept)
		}
	}

	// includeCompleted=true: all 5 children should be returned.
	treeAll, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree includeCompleted=true: %v", err)
	}
	if len(treeAll.Children) != 5 {
		t.Fatalf("includeCompleted=true: got %d children, want 5", len(treeAll.Children))
	}
}

// TestRememberTree_BatchPath verifies that RememberTree uses the batch write
// path correctly: a tree with root + 2 children produces the expected IDs,
// nodeMap entries, and correctly ordered ordinals upon RecallTree.
func TestRememberTree_BatchPath(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "batch-path-edge"

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "BatchRoot",
			Content: "root node for batch path test",
			Type:    "goal",
			Children: []TreeNodeInput{
				{Concept: "BatchChild1", Content: "first child", Type: "task"},
				{Concept: "BatchChild2", Content: "second child", Type: "task"},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	// root_id must be non-empty.
	if result.RootID == "" {
		t.Fatal("expected non-empty RootID")
	}

	// nodeMap must have exactly 3 entries (root + 2 children).
	if len(result.NodeMap) != 3 {
		t.Fatalf("NodeMap: got %d entries, want 3", len(result.NodeMap))
	}
	for _, concept := range []string{"BatchRoot", "BatchChild1", "BatchChild2"} {
		if _, ok := result.NodeMap[concept]; !ok {
			t.Errorf("NodeMap missing concept %q", concept)
		}
	}

	// RecallTree using the returned root_id must return 2 children with correct
	// ordinals: BatchChild1 → ordinal 1, BatchChild2 → ordinal 2.
	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}
	if tree.Concept != "BatchRoot" {
		t.Errorf("root concept: got %q, want BatchRoot", tree.Concept)
	}
	if len(tree.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(tree.Children))
	}
	if tree.Children[0].Concept != "BatchChild1" {
		t.Errorf("child[0] concept: got %q, want BatchChild1", tree.Children[0].Concept)
	}
	if tree.Children[0].Ordinal != 1 {
		t.Errorf("child[0] ordinal: got %d, want 1", tree.Children[0].Ordinal)
	}
	if tree.Children[1].Concept != "BatchChild2" {
		t.Errorf("child[1] concept: got %q, want BatchChild2", tree.Children[1].Concept)
	}
	if tree.Children[1].Ordinal != 2 {
		t.Errorf("child[1] ordinal: got %d, want 2", tree.Children[1].Ordinal)
	}
}

// TestAddChild_OnSoftDeletedParent verifies that AddChild rejects a soft-deleted parent.
// The operation must return an error containing "state" or "deleted" — no child is written.
func TestAddChild_OnSoftDeletedParent(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "soft-deleted-parent-edge"

	// Create a parent engram.
	resp, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   vault,
		Concept: "ParentToDelete",
		Content: "this parent will be soft-deleted",
	})
	if err != nil {
		t.Fatalf("Write parent: %v", err)
	}
	parentID := resp.ID

	// Soft-delete the parent.
	if _, err := eng.Forget(ctx, &mbp.ForgetRequest{
		ID:    parentID,
		Hard:  false,
		Vault: vault,
	}); err != nil {
		t.Fatalf("Forget (soft delete): %v", err)
	}

	// Attempt to add a child — must fail.
	_, err = eng.AddChild(ctx, vault, parentID, &AddChildInput{
		Concept: "OrphanChild",
		Content: "child of a deleted parent",
	})
	if err == nil {
		t.Fatal("expected error when adding child to soft-deleted parent, got nil")
	}
	if !strings.Contains(err.Error(), "state") && !strings.Contains(err.Error(), "deleted") {
		t.Errorf("expected error to mention state or deleted, got: %v", err)
	}
}

// TestAddChild_OnHardDeletedParent verifies that AddChild rejects a hard-deleted parent.
// Hard delete removes the engram entirely; GetEngram returns an error.
// AddChild must return an error containing "not found".
func TestAddChild_OnHardDeletedParent(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "hard-deleted-parent-edge"

	// Create a parent engram.
	resp, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   vault,
		Concept: "ParentToHardDelete",
		Content: "this parent will be hard-deleted",
	})
	if err != nil {
		t.Fatalf("Write parent: %v", err)
	}
	parentID := resp.ID

	// Hard-delete the parent.
	if _, err := eng.Forget(ctx, &mbp.ForgetRequest{
		ID:    parentID,
		Hard:  true,
		Vault: vault,
	}); err != nil {
		t.Fatalf("Forget (hard delete): %v", err)
	}

	// Attempt to add a child — must fail.
	_, err = eng.AddChild(ctx, vault, parentID, &AddChildInput{
		Concept: "OrphanChild",
		Content: "child of a hard-deleted parent",
	})
	if err == nil {
		t.Fatal("expected error when adding child to hard-deleted parent, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected error to mention 'not found', got: %v", err)
	}
}

// TestRecallTree_FilterSoftDeleted verifies that soft-deleted children are
// excluded when includeCompleted=false, even though soft-deleted is a different
// state from completed (StateSoftDeleted=0x7F, StateCompleted=0x04).
//
// Tree: root with 4 children — 2 active, 1 completed, 1 soft-deleted.
// With includeCompleted=false: only the 2 active children should be returned.
func TestRecallTree_FilterSoftDeleted(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "filter-soft-deleted-edge"

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Root",
			Content: "root node",
			Children: []TreeNodeInput{
				{Concept: "Active1", Content: "active child 1"},
				{Concept: "Active2", Content: "active child 2"},
				{Concept: "Completed1", Content: "completed child"},
				{Concept: "SoftDeleted1", Content: "soft-deleted child"},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	// Mark one child as completed.
	completedID, ok := result.NodeMap["Completed1"]
	if !ok {
		t.Fatal("Completed1 not found in NodeMap")
	}
	if err := eng.UpdateLifecycleState(ctx, vault, completedID, "completed"); err != nil {
		t.Fatalf("UpdateLifecycleState(completed): %v", err)
	}

	// Soft-delete one child via Forget (soft=true, Hard=false).
	softDeletedID, ok := result.NodeMap["SoftDeleted1"]
	if !ok {
		t.Fatal("SoftDeleted1 not found in NodeMap")
	}
	if _, err := eng.Forget(ctx, &mbp.ForgetRequest{
		ID:    softDeletedID,
		Hard:  false,
		Vault: vault,
	}); err != nil {
		t.Fatalf("Forget (soft delete): %v", err)
	}

	// includeCompleted=false: only Active1 and Active2 should be returned.
	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, false)
	if err != nil {
		t.Fatalf("RecallTree includeCompleted=false: %v", err)
	}
	if tree.Concept != "Root" {
		t.Errorf("root concept: got %q, want Root", tree.Concept)
	}
	if len(tree.Children) != 2 {
		t.Fatalf("includeCompleted=false: got %d children, want 2 (active only)", len(tree.Children))
	}
	for _, ch := range tree.Children {
		if ch.Concept == "Completed1" {
			t.Errorf("completed child %q should have been filtered out", ch.Concept)
		}
		if ch.Concept == "SoftDeleted1" {
			t.Errorf("soft-deleted child %q should have been filtered out", ch.Concept)
		}
	}

	// includeCompleted=true: all 4 children should appear (soft-deleted is still in ordinal index).
	treeAll, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree includeCompleted=true: %v", err)
	}
	if len(treeAll.Children) != 4 {
		t.Fatalf("includeCompleted=true: got %d children, want 4", len(treeAll.Children))
	}
}

// TestRememberTree_WhitespaceConcept verifies that a root or child with a
// whitespace-only concept (e.g., "   ") is rejected before any writes occur,
// with an error containing "empty concept".
func TestRememberTree_WhitespaceConcept(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "whitespace-concept-edge"

	// Whitespace-only root concept.
	_, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "   ",
			Content: "root content",
		},
	})
	if err == nil {
		t.Fatal("expected error for whitespace-only root concept, got nil")
	}
	if !strings.Contains(err.Error(), "empty concept") {
		t.Errorf("expected error to contain %q, got: %v", "empty concept", err)
	}

	// Whitespace-only child concept.
	_, err = eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "ValidRoot",
			Content: "root content",
			Children: []TreeNodeInput{
				{
					Concept: "\t\n",
					Content: "child content",
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for whitespace-only child concept, got nil")
	}
	if !strings.Contains(err.Error(), "empty concept") {
		t.Errorf("expected error to contain %q, got: %v", "empty concept", err)
	}
}
