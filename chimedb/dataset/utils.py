"""Dataset utils and click scripts."""

import click

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
