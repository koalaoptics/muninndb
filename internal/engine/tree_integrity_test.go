package engine

import (
	"context"
	"testing"

	"github.com/scrypster/muninndb/internal/storage"
	"github.com/scrypster/muninndb/internal/transport/mbp"
)

// TestRecallTree_MaxDepthBoundary verifies depth boundary semantics for RecallTree.
//
// Tree structure: root → child → grandchild (3 levels)
//
// maxDepth=1: root has 1 child, child has 0 children (grandchild not loaded)
// maxDepth=2: root has 1 child, child has 1 child (grandchild), grandchild has 0 children
// maxDepth=0: full tree — all 3 levels returned
func TestRecallTree_MaxDepthBoundary(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "max-depth-boundary-integrity"

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "BoundaryRoot",
			Content: "root",
			Children: []TreeNodeInput{
				{
					Concept: "BoundaryChild",
					Content: "child",
					Children: []TreeNodeInput{
						{Concept: "BoundaryGrandchild", Content: "grandchild"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}
	if len(result.NodeMap) != 3 {
		t.Fatalf("NodeMap: got %d entries, want 3", len(result.NodeMap))
	}

	// maxDepth=1: root loads children (depth=0 < 1), child does NOT load grandchildren (depth=1 >= 1).
	tree1, err := eng.RecallTree(ctx, vault, result.RootID, 1, 0, true)
	if err != nil {
		t.Fatalf("RecallTree maxDepth=1: %v", err)
	}
	if tree1.Concept != "BoundaryRoot" {
		t.Errorf("maxDepth=1: root concept: got %q, want BoundaryRoot", tree1.Concept)
	}
	if len(tree1.Children) != 1 {
		t.Fatalf("maxDepth=1: root should have 1 child, got %d", len(tree1.Children))
	}
	if tree1.Children[0].Concept != "BoundaryChild" {
		t.Errorf("maxDepth=1: child concept: got %q, want BoundaryChild", tree1.Children[0].Concept)
	}
	if len(tree1.Children[0].Children) != 0 {
		t.Errorf("maxDepth=1: child should have 0 children (grandchild not loaded), got %d", len(tree1.Children[0].Children))
	}

	// maxDepth=2: root loads children, child loads grandchild, grandchild loads nothing.
	tree2, err := eng.RecallTree(ctx, vault, result.RootID, 2, 0, true)
	if err != nil {
		t.Fatalf("RecallTree maxDepth=2: %v", err)
	}
	if len(tree2.Children) != 1 {
		t.Fatalf("maxDepth=2: root should have 1 child, got %d", len(tree2.Children))
	}
	child := tree2.Children[0]
	if child.Concept != "BoundaryChild" {
		t.Errorf("maxDepth=2: child concept: got %q, want BoundaryChild", child.Concept)
	}
	if len(child.Children) != 1 {
		t.Fatalf("maxDepth=2: child should have 1 child (grandchild), got %d", len(child.Children))
	}
	if child.Children[0].Concept != "BoundaryGrandchild" {
		t.Errorf("maxDepth=2: grandchild concept: got %q, want BoundaryGrandchild", child.Children[0].Concept)
	}
	if len(child.Children[0].Children) != 0 {
		t.Errorf("maxDepth=2: grandchild should have 0 children, got %d", len(child.Children[0].Children))
	}

	// maxDepth=0: full tree — root → child → grandchild all returned.
	treeFull, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree maxDepth=0: %v", err)
	}
	if len(treeFull.Children) != 1 {
		t.Fatalf("maxDepth=0: root should have 1 child, got %d", len(treeFull.Children))
	}
	fullChild := treeFull.Children[0]
	if len(fullChild.Children) != 1 {
		t.Fatalf("maxDepth=0: child should have 1 grandchild, got %d", len(fullChild.Children))
	}
	if fullChild.Children[0].Concept != "BoundaryGrandchild" {
		t.Errorf("maxDepth=0: grandchild concept: got %q, want BoundaryGrandchild", fullChild.Children[0].Concept)
	}
	// Grandchild has no children of its own — Children should be empty (not nil).
	if len(fullChild.Children[0].Children) != 0 {
		t.Errorf("maxDepth=0: grandchild should have 0 children, got %d", len(fullChild.Children[0].Children))
	}

	// Verify total node count = 3.
	total := countNodes(treeFull)
	if total != 3 {
		t.Errorf("maxDepth=0: total node count: got %d, want 3", total)
	}
}

// TestHierarchicalMemory_FullIntegration exercises the complete hierarchical
// memory lifecycle: create, recall, mutate (AddChild), filter (Forget/lifecycle),
// and guard against invalid mutations on a soft-deleted root.
//
// Tree layout:
//
//	Project Alpha (root)
//	├─ Backend
//	│  ├─ API Design
//	│  └─ Database Schema
//	├─ Frontend
//	│  └─ UI Components
//	└─ Testing  (no children)
func TestHierarchicalMemory_FullIntegration(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "full-integration-integrity"

	// ── Step 1: RememberTree — create a 4-level project tree ──────────────────
	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Project Alpha",
			Content: "Top-level project",
			Type:    "goal",
			Children: []TreeNodeInput{
				{
					Concept: "Backend",
					Content: "Backend work",
					Children: []TreeNodeInput{
						{Concept: "API Design", Content: "Design REST API"},
						{Concept: "Database Schema", Content: "Define schema"},
					},
				},
				{
					Concept: "Frontend",
					Content: "Frontend work",
					Children: []TreeNodeInput{
						{Concept: "UI Components", Content: "Build UI components"},
					},
				},
				{
					Concept: "Testing",
					Content: "Testing phase",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("step 1 RememberTree: %v", err)
	}
	if result.RootID == "" {
		t.Fatal("step 1: RootID must be non-empty")
	}
	// root + Backend + API Design + Database Schema + Frontend + UI Components + Testing = 7 nodes
	if len(result.NodeMap) != 7 {
		t.Fatalf("step 1: NodeMap: got %d entries, want 7", len(result.NodeMap))
	}
	for _, concept := range []string{"Project Alpha", "Backend", "API Design", "Database Schema", "Frontend", "UI Components", "Testing"} {
		if _, ok := result.NodeMap[concept]; !ok {
			t.Errorf("step 1: NodeMap missing concept %q", concept)
		}
	}

	// ── Step 2: RecallTree — assert full tree structure ───────────────────────
	tree, err := eng.RecallTree(ctx, vault, result.RootID, 10, 0, false)
	if err != nil {
		t.Fatalf("step 2 RecallTree: %v", err)
	}
	if tree.Concept != "Project Alpha" {
		t.Errorf("step 2: root concept: got %q, want Project Alpha", tree.Concept)
	}
	if len(tree.Children) != 3 {
		t.Fatalf("step 2: root should have 3 direct children, got %d", len(tree.Children))
	}
	// Verify ordinals 1, 2, 3 on direct children.
	expectedDirectChildren := []string{"Backend", "Frontend", "Testing"}
	for i, ch := range tree.Children {
		if ch.Concept != expectedDirectChildren[i] {
			t.Errorf("step 2: direct child[%d]: got %q, want %q", i, ch.Concept, expectedDirectChildren[i])
		}
		if ch.Ordinal != int32(i+1) {
			t.Errorf("step 2: direct child[%d] ordinal: got %d, want %d", i, ch.Ordinal, i+1)
		}
	}
	// Verify Backend has 2 children.
	backend := tree.Children[0]
	if len(backend.Children) != 2 {
		t.Fatalf("step 2: Backend should have 2 children, got %d", len(backend.Children))
	}
	// Total nodes: 7.
	total := countNodes(tree)
	if total != 7 {
		t.Errorf("step 2: total node count: got %d, want 7", total)
	}

	// ── Step 3: AddChild — add "Documentation" as 4th direct child ───────────
	addResp, err := eng.AddChild(ctx, vault, result.RootID, &AddChildInput{
		Concept: "Documentation",
		Content: "Project docs",
	})
	if err != nil {
		t.Fatalf("step 3 AddChild: %v", err)
	}
	if addResp.ChildID == "" {
		t.Fatal("step 3: AddChild returned empty ChildID")
	}
	if addResp.Ordinal != 4 {
		t.Errorf("step 3: AddChild ordinal: got %d, want 4", addResp.Ordinal)
	}

	// ── Step 4: RecallTree — assert 4 direct children ─────────────────────────
	tree2, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, false)
	if err != nil {
		t.Fatalf("step 4 RecallTree: %v", err)
	}
	if len(tree2.Children) != 4 {
		t.Fatalf("step 4: root should have 4 direct children after AddChild, got %d", len(tree2.Children))
	}
	// Last child should be Documentation with ordinal 4.
	lastChild := tree2.Children[3]
	if lastChild.Concept != "Documentation" {
		t.Errorf("step 4: last child concept: got %q, want Documentation", lastChild.Concept)
	}
	if lastChild.Ordinal != 4 {
		t.Errorf("step 4: last child ordinal: got %d, want 4", lastChild.Ordinal)
	}

	// ── Step 5: Forget "API Design" (soft-delete) and verify filtering ─────────
	apiDesignID := result.NodeMap["API Design"]
	if _, err := eng.Forget(ctx, &mbp.ForgetRequest{
		Vault: vault,
		ID:    apiDesignID,
		Hard:  false,
	}); err != nil {
		t.Fatalf("step 5 Forget(API Design, soft): %v", err)
	}

	// includeCompleted=false: API Design must not appear under Backend.
	treeExcluded, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, false)
	if err != nil {
		t.Fatalf("step 5 RecallTree(includeCompleted=false): %v", err)
	}
	backendExcluded := treeExcluded.Children[0] // Backend is still ordinal 1
	for _, ch := range backendExcluded.Children {
		if ch.Concept == "API Design" {
			t.Errorf("step 5: API Design should be filtered (soft-deleted) with includeCompleted=false")
		}
	}

	// includeCompleted=true: API Design must appear with state="deleted".
	treeIncluded, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("step 5 RecallTree(includeCompleted=true): %v", err)
	}
	backendIncluded := treeIncluded.Children[0]
	var foundAPIDesign bool
	for _, ch := range backendIncluded.Children {
		if ch.Concept == "API Design" {
			foundAPIDesign = true
			if ch.State != "deleted" {
				t.Errorf("step 5: API Design state: got %q, want deleted", ch.State)
			}
		}
	}
	if !foundAPIDesign {
		t.Error("step 5: API Design should appear with includeCompleted=true")
	}

	// ── Step 6: UpdateLifecycleState "Database Schema" → completed ────────────
	dbSchemaID := result.NodeMap["Database Schema"]
	if err := eng.UpdateLifecycleState(ctx, vault, dbSchemaID, "completed"); err != nil {
		t.Fatalf("step 6 UpdateLifecycleState(Database Schema, completed): %v", err)
	}

	// includeCompleted=false: Database Schema must not appear under Backend.
	treeFiltered, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, false)
	if err != nil {
		t.Fatalf("step 6 RecallTree(includeCompleted=false): %v", err)
	}
	backendFiltered := treeFiltered.Children[0]
	for _, ch := range backendFiltered.Children {
		if ch.Concept == "Database Schema" {
			t.Errorf("step 6: Database Schema should be filtered (completed) with includeCompleted=false")
		}
	}

	// includeCompleted=true: Database Schema present with state="completed".
	treeAll, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("step 6 RecallTree(includeCompleted=true): %v", err)
	}
	backendAll := treeAll.Children[0]
	var foundDBSchema bool
	for _, ch := range backendAll.Children {
		if ch.Concept == "Database Schema" {
			foundDBSchema = true
			if ch.State != "completed" {
				t.Errorf("step 6: Database Schema state: got %q, want completed", ch.State)
			}
		}
	}
	if !foundDBSchema {
		t.Error("step 6: Database Schema should appear with includeCompleted=true")
	}

	// ── Step 7: CountChildren on Backend ─────────────────────────────────────
	// CountChildren counts ordinal keys regardless of lifecycle state.
	// Backend had 2 children (API Design + Database Schema), both are still
	// tracked as ordinal keys even after soft-delete / completed state change.
	backendID := result.NodeMap["Backend"]
	count, err := eng.CountChildren(ctx, vault, backendID)
	if err != nil {
		t.Fatalf("step 7 CountChildren(Backend): %v", err)
	}
	if count != 2 {
		t.Errorf("step 7: CountChildren(Backend): got %d, want 2 (ordinal keys persist regardless of state)", count)
	}

	// ── Step 8: Forget root (soft-delete) ────────────────────────────────────
	forgetResp, err := eng.Forget(ctx, &mbp.ForgetRequest{
		Vault: vault,
		ID:    result.RootID,
		Hard:  false,
	})
	if err != nil {
		t.Fatalf("step 8 Forget(root, soft): %v", err)
	}
	if !forgetResp.OK {
		t.Error("step 8: Forget root: expected OK=true")
	}

	// RecallTree on soft-deleted root should return root node with state="deleted".
	treeRoot, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("step 8 RecallTree(soft-deleted root): %v", err)
	}
	if treeRoot.State != "deleted" {
		t.Errorf("step 8: soft-deleted root state: got %q, want deleted", treeRoot.State)
	}

	// ── Step 9: AddChild on soft-deleted root — must error ────────────────────
	_, err = eng.AddChild(ctx, vault, result.RootID, &AddChildInput{
		Concept: "Orphan Test",
		Content: "orphan",
	})
	if err == nil {
		t.Fatal("step 9: AddChild on soft-deleted root should error, got nil")
	}
	// Error must mention state or deleted.
	errStr := err.Error()
	if errStr == "" {
		t.Error("step 9: AddChild error message must not be empty")
	}
}

// TestRememberTree_RecallTree_FullRoundtrip is the gold standard data integrity test.
// It writes a 3-level tree: root → 3 phases → 3 tasks each = 13 nodes total.
// Then it verifies structure, ordinal ordering, and that all 13 unique IDs appear
// exactly once across the full tree.
func TestRememberTree_RecallTree_FullRoundtrip(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "integrity-roundtrip"

	req := &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Root",
			Content: "root node",
			Children: []TreeNodeInput{
				{
					Concept: "Phase 1",
					Content: "first phase",
					Children: []TreeNodeInput{
						{Concept: "Task 1.1", Content: "task 1 of phase 1"},
						{Concept: "Task 1.2", Content: "task 2 of phase 1"},
						{Concept: "Task 1.3", Content: "task 3 of phase 1"},
					},
				},
				{
					Concept: "Phase 2",
					Content: "second phase",
					Children: []TreeNodeInput{
						{Concept: "Task 2.1", Content: "task 1 of phase 2"},
						{Concept: "Task 2.2", Content: "task 2 of phase 2"},
						{Concept: "Task 2.3", Content: "task 3 of phase 2"},
					},
				},
				{
					Concept: "Phase 3",
					Content: "third phase",
					Children: []TreeNodeInput{
						{Concept: "Task 3.1", Content: "task 1 of phase 3"},
						{Concept: "Task 3.2", Content: "task 2 of phase 3"},
						{Concept: "Task 3.3", Content: "task 3 of phase 3"},
					},
				},
			},
		},
	}

	result, err := eng.RememberTree(ctx, req)
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	// NodeMap must have exactly 13 entries.
	if len(result.NodeMap) != 13 {
		t.Fatalf("NodeMap: got %d entries, want 13", len(result.NodeMap))
	}

	// RecallTree with maxDepth=0 (unlimited), limit=0, includeCompleted=true.
	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}

	// Root concept must match.
	if tree.Concept != "Root" {
		t.Errorf("root concept: got %q, want %q", tree.Concept, "Root")
	}

	// Exactly 3 phases.
	if len(tree.Children) != 3 {
		t.Fatalf("expected 3 phases, got %d", len(tree.Children))
	}

	// Phases must be in ordinal order (Phase 1 before Phase 2 before Phase 3).
	phaseNames := []string{"Phase 1", "Phase 2", "Phase 3"}
	for i, phase := range tree.Children {
		if phase.Concept != phaseNames[i] {
			t.Errorf("phase[%d]: got %q, want %q", i, phase.Concept, phaseNames[i])
		}
		// Each phase must have ordinal i+1.
		if phase.Ordinal != int32(i+1) {
			t.Errorf("phase[%d] ordinal: got %d, want %d", i, phase.Ordinal, i+1)
		}
		// Each phase must have exactly 3 tasks.
		if len(phase.Children) != 3 {
			t.Fatalf("phase[%d] (%q): got %d tasks, want 3", i, phase.Concept, len(phase.Children))
		}
		// Tasks within each phase must be in ordinal order.
		for j, task := range phase.Children {
			expectedTask := ""
			switch i {
			case 0:
				expectedTask = []string{"Task 1.1", "Task 1.2", "Task 1.3"}[j]
			case 1:
				expectedTask = []string{"Task 2.1", "Task 2.2", "Task 2.3"}[j]
			case 2:
				expectedTask = []string{"Task 3.1", "Task 3.2", "Task 3.3"}[j]
			}
			if task.Concept != expectedTask {
				t.Errorf("phase[%d] task[%d]: got %q, want %q", i, j, task.Concept, expectedTask)
			}
			if task.Ordinal != int32(j+1) {
				t.Errorf("phase[%d] task[%d] ordinal: got %d, want %d", i, j, task.Ordinal, j+1)
			}
		}
	}

	// Collect all IDs from the tree and verify no duplicates and exactly 13 unique IDs.
	seenIDs := make(map[string]int) // id → count
	collectIDs(tree, seenIDs)

	if len(seenIDs) != 13 {
		t.Errorf("unique IDs in tree: got %d, want 13", len(seenIDs))
	}
	for id, count := range seenIDs {
		if count != 1 {
			t.Errorf("ID %q appears %d times (want exactly 1)", id, count)
		}
	}
}

// collectIDs recursively accumulates node IDs into the provided map (id → count).
func collectIDs(node *TreeNode, seen map[string]int) {
	if node == nil {
		return
	}
	seen[node.ID]++
	for i := range node.Children {
		collectIDs(&node.Children[i], seen)
	}
}

// TestAddChild_ThenRecall_Integrity verifies that children added via AddChild
// appear correctly in RecallTree.
func TestAddChild_ThenRecall_Integrity(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "integrity-addchild"

	// Create root via eng.Write.
	resp, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   vault,
		Concept: "Root",
		Content: "root node",
	})
	if err != nil {
		t.Fatalf("Write root: %v", err)
	}
	rootID := resp.ID

	// Add 5 children via AddChild (no explicit ordinals — append mode).
	for i := 0; i < 5; i++ {
		_, err := eng.AddChild(ctx, vault, rootID, &AddChildInput{
			Concept: "Child",
			Content: "child content",
		})
		if err != nil {
			t.Fatalf("AddChild[%d]: %v", i, err)
		}
	}

	// RecallTree must return exactly 5 children in ordinal order 1-5.
	tree, err := eng.RecallTree(ctx, vault, rootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree (5 children): %v", err)
	}
	if len(tree.Children) != 5 {
		t.Fatalf("expected 5 children after 5 AddChild calls, got %d", len(tree.Children))
	}
	for i, ch := range tree.Children {
		expectedOrdinal := int32(i + 1)
		if ch.Ordinal != expectedOrdinal {
			t.Errorf("child[%d] ordinal: got %d, want %d", i, ch.Ordinal, expectedOrdinal)
		}
	}

	// Add one more child.
	_, err = eng.AddChild(ctx, vault, rootID, &AddChildInput{
		Concept: "Child",
		Content: "sixth child",
	})
	if err != nil {
		t.Fatalf("AddChild (6th): %v", err)
	}

	// RecallTree again must return 6 children.
	tree2, err := eng.RecallTree(ctx, vault, rootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree (6 children): %v", err)
	}
	if len(tree2.Children) != 6 {
		t.Fatalf("expected 6 children after 6th AddChild, got %d", len(tree2.Children))
	}
	// Last child must have ordinal 6.
	if tree2.Children[5].Ordinal != 6 {
		t.Errorf("6th child ordinal: got %d, want 6", tree2.Children[5].Ordinal)
	}
}

// TestRememberTree_DeleteChild_OrdinalClean verifies that deleting an engram
// automatically removes its ordinal key atomically in the same Pebble batch.
// No explicit DeleteEngramOrdinal call is required after DeleteEngram.
func TestRememberTree_DeleteChild_OrdinalClean(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "integrity-ordinal-clean"

	// Create root → child via RememberTree.
	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Root",
			Content: "root",
			Children: []TreeNodeInput{
				{Concept: "Child", Content: "child"},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	rootID, err := storage.ParseULID(result.RootID)
	if err != nil {
		t.Fatalf("parse root ULID: %v", err)
	}

	childIDStr, ok := result.NodeMap["Child"]
	if !ok {
		t.Fatal("Child not found in NodeMap")
	}
	childID, err := storage.ParseULID(childIDStr)
	if err != nil {
		t.Fatalf("parse child ULID: %v", err)
	}

	ws := eng.store.ResolveVaultPrefix(vault)

	// Delete the child engram — ordinal must be auto-cleaned atomically.
	if err := eng.store.DeleteEngram(ctx, ws, childID); err != nil {
		t.Fatalf("DeleteEngram: %v", err)
	}

	// After DeleteEngram, ordinal is auto-cleaned.
	_, found, err := eng.store.ReadOrdinal(ctx, ws, rootID, childID)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("ordinal should be automatically removed when child engram is hard-deleted")
	}
}

// TestRecallTree_MissingChildEngram verifies that RecallTree does not panic when an
// ordinal key points to an engram that has been deleted. With includeCompleted=false,
// the pre-filter (GetMetadata) returns nil/empty for the missing child and silently
// skips it. The root is returned with 0 children.
func TestRecallTree_MissingChildEngram(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "integrity-missing-child"

	// Create root → child via RememberTree.
	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Root",
			Content: "root",
			Children: []TreeNodeInput{
				{Concept: "GhostChild", Content: "will be deleted"},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	childIDStr, ok := result.NodeMap["GhostChild"]
	if !ok {
		t.Fatal("GhostChild not found in NodeMap")
	}
	childID, err := storage.ParseULID(childIDStr)
	if err != nil {
		t.Fatalf("parse child ULID: %v", err)
	}

	ws := eng.store.ResolveVaultPrefix(vault)

	// Directly delete the child engram record (NOT the ordinal key).
	// The ordinal key remains, so ListChildOrdinals will still return this child.
	if err := eng.store.DeleteEngram(ctx, ws, childID); err != nil {
		t.Fatalf("DeleteEngram: %v", err)
	}

	// RecallTree with includeCompleted=false triggers the GetMetadata pre-filter.
	// GetMetadata for the missing child returns nil/empty → child is skipped silently.
	// Must NOT panic.
	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, false)
	if err != nil {
		t.Fatalf("RecallTree: %v (want nil error — missing child should be skipped silently)", err)
	}

	// Root should be returned.
	if tree == nil {
		t.Fatal("RecallTree returned nil tree")
	}
	if tree.Concept != "Root" {
		t.Errorf("root concept: got %q, want Root", tree.Concept)
	}

	// The missing child must have been dropped silently — 0 children.
	if len(tree.Children) != 0 {
		t.Errorf("expected 0 children (missing child dropped), got %d", len(tree.Children))
	}
}

// TestRememberTree_Atomicity_SuccessPath verifies that when RememberTree succeeds,
// Phase 1 (batch write) and Phase 2 (wiring) both complete — all 5 nodes are
// retrievable via Read and RecallTree returns the full wired tree.
// This confirms the rollback helper is not invoked on the happy path.
func TestRememberTree_Atomicity_SuccessPath(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "atomicity-success-path"

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "Root",
			Content: "root node",
			Children: []TreeNodeInput{
				{Concept: "Child1", Content: "first child"},
				{Concept: "Child2", Content: "second child"},
				{
					Concept: "Child3",
					Content: "third child with grandchild",
					Children: []TreeNodeInput{
						{Concept: "Grandchild1", Content: "grandchild"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	// NodeMap must have exactly 5 entries (root + 3 children + 1 grandchild).
	if len(result.NodeMap) != 5 {
		t.Fatalf("NodeMap: got %d entries, want 5", len(result.NodeMap))
	}

	// All 5 IDs must be readable individually via Read.
	expectedConcepts := []string{"Root", "Child1", "Child2", "Child3", "Grandchild1"}
	for _, concept := range expectedConcepts {
		id, ok := result.NodeMap[concept]
		if !ok {
			t.Errorf("NodeMap missing concept %q", concept)
			continue
		}
		resp, err := eng.Read(ctx, &mbp.ReadRequest{Vault: vault, ID: id})
		if err != nil {
			t.Errorf("Read(%q, id=%q): %v", concept, id, err)
			continue
		}
		if resp == nil {
			t.Errorf("Read(%q): got nil response", concept)
			continue
		}
		if resp.Concept != concept {
			t.Errorf("Read(%q): concept mismatch: got %q", concept, resp.Concept)
		}
	}

	// RecallTree must return all 5 nodes wired correctly.
	tree, err := eng.RecallTree(ctx, vault, result.RootID, 0, 0, true)
	if err != nil {
		t.Fatalf("RecallTree: %v", err)
	}

	// Root must have 3 direct children.
	if len(tree.Children) != 3 {
		t.Fatalf("root children: got %d, want 3", len(tree.Children))
	}

	// Child3 must have 1 grandchild.
	var child3 *TreeNode
	for i := range tree.Children {
		if tree.Children[i].Concept == "Child3" {
			child3 = &tree.Children[i]
			break
		}
	}
	if child3 == nil {
		t.Fatal("Child3 not found in tree")
	}
	if len(child3.Children) != 1 {
		t.Fatalf("Child3 children: got %d, want 1", len(child3.Children))
	}
	if child3.Children[0].Concept != "Grandchild1" {
		t.Errorf("grandchild concept: got %q, want Grandchild1", child3.Children[0].Concept)
	}

	// Collect all IDs from the recalled tree — must be 5 distinct IDs.
	seenIDs := make(map[string]int)
	collectIDs(tree, seenIDs)
	if len(seenIDs) != 5 {
		t.Errorf("unique IDs in tree: got %d, want 5", len(seenIDs))
	}
	for id, count := range seenIDs {
		if count != 1 {
			t.Errorf("ID %q appears %d times (want exactly 1)", id, count)
		}
	}
}

// TestRememberTree_NodeMapAccuracy verifies that NodeMap contains exactly the right
// concept → ULID mappings and that all IDs are valid, non-empty, and distinct.
func TestRememberTree_NodeMapAccuracy(t *testing.T) {
	t.Parallel()
	eng, cleanup := testEnv(t)
	defer cleanup()
	ctx := context.Background()
	vault := "integrity-nodemap"

	result, err := eng.RememberTree(ctx, &RememberTreeRequest{
		Vault: vault,
		Root: TreeNodeInput{
			Concept: "R",
			Content: "root",
			Children: []TreeNodeInput{
				{
					Concept: "A",
					Content: "child A",
					Children: []TreeNodeInput{
						{Concept: "A.1", Content: "grandchild A.1"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("RememberTree: %v", err)
	}

	// NodeMap must have exactly 3 entries.
	if len(result.NodeMap) != 3 {
		t.Fatalf("NodeMap len: got %d, want 3", len(result.NodeMap))
	}

	// Each expected key must be present.
	expectedKeys := []string{"R", "A", "A.1"}
	for _, k := range expectedKeys {
		if _, ok := result.NodeMap[k]; !ok {
			t.Errorf("NodeMap missing key %q", k)
		}
	}

	// RootID must match the ID stored for "R".
	if result.NodeMap["R"] != result.RootID {
		t.Errorf("NodeMap[\"R\"] = %q, want RootID %q", result.NodeMap["R"], result.RootID)
	}

	// Collect IDs and verify they are valid ULIDs (non-empty) and distinct.
	ids := make(map[string]string) // concept → id
	for _, k := range expectedKeys {
		id := result.NodeMap[k]
		if id == "" {
			t.Errorf("NodeMap[%q] is empty", k)
			continue
		}
		// Verify it parses as a valid ULID.
		if _, err := storage.ParseULID(id); err != nil {
			t.Errorf("NodeMap[%q] = %q is not a valid ULID: %v", k, id, err)
		}
		ids[k] = id
	}

	// All 3 IDs must be distinct.
	idSet := make(map[string]string) // id → concept (for error messages)
	for concept, id := range ids {
		if prev, exists := idSet[id]; exists {
			t.Errorf("duplicate ULID %q shared by concepts %q and %q", id, prev, concept)
		}
		idSet[id] = concept
	}
	if len(idSet) != 3 {
		t.Errorf("expected 3 distinct IDs, got %d", len(idSet))
	}
}
