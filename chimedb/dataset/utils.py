"""Dataset utils and click scripts."""

import click

import numpy as np

from chimedb.core import connect as connect_db, close as close_db
from chimedb.dataset.get import Dataset, DatasetCache, index


@click.group()
def cli():
    """Click entry point."""
    pass


@cli.command()
@click.argument("dataset_id")
def treesize(dataset_id):
    """Print number of nodes in the tree containing DATASET_ID."""
    click.echo(f"Counting tree size of node {dataset_id}...")
    connect_db()

    # Get all datasets cached from DB
    all_nodes = DatasetCache()

    click.echo(f"Total number of nodes in DB: {len(all_nodes)}")

    start_node = Dataset.from_id(dataset_id)
    tree = set()

    # traverse to root first
    node = start_node
    tree.add(node)
    while not node.root:
        node = node.base_dataset
        tree.add(node)

    for n in all_nodes.values():
        if n in tree:
            continue
        if in_tree(n, tree):
            tree.add(n)

    close_db()
    click.echo(f"Tree size: {len(tree)}")


def in_tree(node, tree):
    """
    Tell if a node is part of a tree.

    Parameters
    ----------
    node : Dataset
        The node to check for.
    tree : set
        The tree to look in.

    Returns
    -------
    bool
        True, if the node is found in the given tree.
    """
    if node in tree:
        return True

    while not node.root:
        node = node.base_dataset
        if node in tree:
            return True

    return False


def state_id_of_type(ds_ids: np.ndarray, state_type: str) -> np.ma.MaskedArray:
    """For an array of dataset IDs look up the corresponding state ID.

    Parameters
    ----------
    ds_ids
        Array of dataset IDs.
    state_type
        Name of the dataset state type.

    Returns
    -------
    state_ids
        Array of state IDs. If the ds_id is null, then the entry is masked in the
        output.
    """
    nulldset = "00000000000000000000000000000000"

    unique_ds_ids, ds_index = np.unique(ds_ids, return_inverse=True)

    # Fetch the corresponding state, or return null if the ds was null
    def _state_or_null(ds_id):
        if ds_id == nulldset:
            return nulldset
        else:
            return (
                ds.Dataset.from_id(ds_id).closest_ancestor_of_type(state_type).state.id
            )

    state_ids = np.array([_state_or_null(ds_id) for ds_id in unique_ds_ids])

    masked_ids = np.ma.array(
        state_ids[ds_index.reshape(ds_ids.shape)],
        mask=(state_ids == nulldset)[ds_index.reshape(ds_ids.shape)],
    )
    return masked_ids


def unique_unmasked_entry(A: np.ma.MaskedArray, axis: int = -1) -> np.ma.MaskedArray:
    """Return the unique unmasked entry along an axis.

    This tests to see if all unmasked entries along an axis are identical.
    If they are it returns the unique entry, otherwise the entry is masked.

    When combined with `state_id_of_type`, this is particularly useful for determining
    if all frequencies in a stream have an identical state of the given type.

    Parameters
    ----------
    A
        An N-D masked array. The dtype of A must be one that can be compared for
        uniqueness.
    axis
        The axis to test. By default use the last axis.

    Returns
    -------
    unique_entries
        An (N-1)-D with the same shape as `A` with the specified `axis` removed.
    """
    # Use np.unique to process whatever the input type is into a set of integer indices
    # we can manipulate more easily
    A_vals, A_index = np.unique(A.data, return_inverse=True)
    A_index = A_index.reshape(A.shape)
    A_index_masked = np.ma.array(A_index, mask=A.mask)

    # Reduce along the axis to find what the unique value would be (if it was unique)
    # Keep the dimensions to make the next comparison easy
    A_single = A_index_masked.min(axis=axis, keepdims=True)

    # Test that all entries along the axis are equal to the guess above
    unique_entry = (A_index_masked == A_single).all(axis=axis)

    # Remove the extra dimension (can't use squeeze here in case we have genuine length
    # one axes)
    A_single = np.take(A_single, 0, axis=axis)

    # Return a new masked array
    return np.ma.array(
        A_vals[A_single.filled(0)], mask=(A_single.mask | ~unique_entry.data)
    )
