import argparse
import collections
import configparser
from datetime import datetime
import grp, pwd
from fnmatch import fnmatch
import hashlib
from math import ceil
import os
import re
import sys
import zlib

argparser = argparse.ArgumentParser(description="Version control")
argsubparsers = argparser.add_subparsers(title="Commands", dest="command")
argsubparsers.required = True

# Subparser for init
argsp = argsubparsers.add_parser("init", help="Initialize a new, empty repository.")
argsp.add_argument(
    "path",
    metavar="directory",
    nargs="?",
    default=".",
    help="Where to create the repository.",
)


# From the first one, so we skip the "wyag" command
def main(argv=sys.argv[1:]):
    args = argparser.parse_args(argv)
    match args.command:
        case "add":
            cmd_add(args)
        case "cat-file":
            cmd_cat_file(args)
        case "check-ignore":
            cmd_check_ignore(args)
        case "checkout":
            cmd_checkout(args)
        case "commit":
            cmd_commit(args)
        case "hash-object":
            cmd_hash_object(args)
        case "init":
            cmd_init(args)
        case "log":
            cmd_log(args)
        case "ls-files":
            cmd_ls_files(args)
        case "ls-tree":
            cmd_ls_tree(args)
        case "rev-parse":
            cmd_rev_parse(args)
        case "rm":
            cmd_rm(args)
        case "show-ref":
            cmd_show_ref(args)
        case "status":
            cmd_status(args)
        case "tag":
            cmd_tag(args)
        case _:
            print("Bad command.")


class GitRepository(object):
    worktree = None
    gitdir = None
    conf = None

    def __init__(self, path, force=False) -> None:
        self.worktree = path
        self.gitdir = os.path.join(path, ".git")

        if not (force or os.path.isdir(self.gitdir)):
            raise Exception("This is not a Git repository %s" % path)

        # Read configuration file in .git/config
        self.conf = configparser.ConfigParser()
        config = repo_file(self, "config")

        if config and os.path.exists(config):
            self.conf.read(config)
        elif not force:
            raise Exception("Configuration file missing")

        if not force:
            version = int(self.conf.get("core", "repositoryformatversion"))
            if version != 0:
                raise Exception("Unsupported repositoryformatversion %s" % version)


class GitObject(object):
    def __init__(self, data=None) -> None:
        if data != None:
            self.deserialize(data)
        else:
            self.init()

    def serialize(self, repo: GitRepository):
        raise NotImplementedError()

    def deserialize(self, data):
        raise NotImplementedError()

    def init(self):
        pass


def GitBlob(GitObject):
    fmt = b"blob"

    def serialize(self):
        return self.blobdata

    def deserialize(self, data):
        self.blobdata = data


def repo_path(repo, *path):
    return os.path.join(repo.gitdir, *path)


def repo_dir(repo, *path, mkdir=False):
    path = repo_path(repo, path)

    if os.path.exists(path):
        if os.path.isdir(path):
            return path
        else:
            raise Exception("Not a directory %s" % path)

    if mkdir:
        os.makedirs(path)
        return path

    return None


def repo_file(repo, *path, mkdir=False):
    # Create dir if absent
    if repo_dir(repo, *path[:-1], mkdir=mkdir):
        return repo_path(repo, *path)


def repo_create(path) -> GitRepository:
    repo = GitRepository(path, force=True)

    # Make sure the path either doesn't exist or is an empty dir.
    if os.path.exists(repo.worktree):
        if not os.path.isdir(repo.worktree):
            raise Exception("%s is not a directory!" % path)
        if os.path.exists(repo.gitdir) and os.listdir(repo.gitdir):
            raise Exception("%s is not empty!" % path)
    else:
        os.makedirs(repo.worktree)

    assert repo_dir(repo, "branches", mkdir=True)
    assert repo_dir(repo, "objects", mkdir=True)
    assert repo_dir(repo, "refs", "tags", mkdir=True)
    assert repo_dir(repo, "refs", "heads", mkdir=True)

    # .git/description
    with open(repo_file(repo, "description"), "w") as f:
        f.write(
            "Unnamed repository; edit this file 'description' to name the repository.\n"
        )

    # .git/HEAD
    with open(repo_file(repo, "HEAD"), "w") as f:
        f.write("ref: refs/heads/master\n")

    with open(repo_file(repo, "config"), "w") as f:
        config = repo_default_config()
        config.write(f)

    return repo


def repo_default_config() -> configparser.ConfigParser:
    ret = configparser.ConfigParser()

    ret.add_section("core")
    # The version of the gitdir format. 0 means the initial format, 1 the same with extensions
    ret.set("core", "repositoryformatversion", "0")
    # Disable tracking of file mode (permissions) changes in the work tree
    ret.set("core", "filemode", "false")
    # Indicates that this repository has a worktree
    ret.set("core", "bare", "false")

    return ret


def cmd_init(args):
    repo_create(args)


# Find the root of current repository (where's the file .git is present)
def repo_find(path=".", required=True) -> GitRepository:
    path = os.path.realpath(path)

    if os.path.isdir(os.path.join(path, ".git")):
        return GitRepository(path)

    # If we haven't returned, recurse in parent, if w
    parent = os.path.relpath(os.path.join(path, ".."))

    if parent == path:
        # Bottom case
        # os.path.join("/", "..") == "/":
        # If parent==path, then path is root
        if required:
            raise Exception("No git directory.")
        else:
            return None

    # Recursive case
    return repo_find(parent, required)


def object_read(repo: GitRepository, sha) -> GitObject:
    """Read object sha from Git repository repo.  Return a
    GitObject whose exact type depends on the object.

    The path (inside the gitdir) is composed from the "objects" directory,
    then a folder with the first two charactes of the file name, then the file name.

    Object name: e673d1b7eaa0aa01b5bc2442d570a765bdaae751
    Path to object: .git/objects/e6/e673d1b7eaa0aa01b5bc2442d570a765bdaae751
    """
    path = repo_file(repo, "objects", sha[0:2], sha[2:])

    if not os.path.isfile(path):
        return None

    # To read a binary file
    with open(path, "rb") as f:
        raw = zlib.decompress(f.read())

        # Read object type (from 0 to " ")
        x = raw.find(b" ")
        fmt = raw[0:x]

        # Read and validate object size (x00 = null)
        y = raw.find(b"\x00", x)
        size = int(raw[x:y].decode("ascii"))
        if size != len(raw) - y - 1:
            raise Exception("Malformed object {0}: bad length", format(sha))

        # Pick constructor
        match fmt:
            case b"commit":
                c = GitCommit
            case b"tree":
                c = GitTree
            case b"tag":
                c = GitTag
            case b"blob":
                c = GitBlob
            case _:
                raise Exception(
                    "Unkwown type {0} for object {1}", format(fmt.decode("ascii"), sha)
                )

        # Call constructor (without the header) and return object
        return c(raw[y + 1 :])


def object_write(object: GitObject, repo: GitRepository = None) -> str:
    data = object.serialize()
    # Add header
    result = object.fmt + b" " + str(len(data)).encode + b"\x00" + data
    # Compute hash
    sha = hashlib.sha1(result).hexdigest()

    if repo:
        path = repo_file(repo, "objects", sha[0:2], sha[2:], mkdir=True)

        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(zlib.compress(result))
    return sha
