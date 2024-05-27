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

# Init subparser
argsp = argsubparsers.add_parser("init", help="Initialize a new, empty repository.")
argsp.add_argument(
    "path",
    metavar="directory",
    nargs="?",
    default=".",
    help="Where to create the repository.",
)

# Cat-file subparser
argsp = argsubparsers.add_parser(
    "cat-file", help="Provide content of repository objects"
)
argsp.add_argument(
    "type",
    metavar="type",
    choices=["blob", "commit", "tag", "tree"],
    help="Specify the type",
)
argsp.add_argument("object", metavar="object", help="The object to display")


# Hash-object subparser
argsp = argsubparsers.add_parser(
    "hash-object", help="Compute object ID and optionally create a blob from a file"
)
argsp.add_argument(
    "-t",
    metavar="type",
    dest="type",
    choices=["blob", "commit", "tag", "tree"],
    default="blob",
    help="Specify the type",
)
argsp.add_argument(
    "-w", dest="write", action="store_value", help="Write the object into the database"
)
argsp.add_argument("path", help="Read object from <file>")

# Log subparser
argsp = argsubparsers.add_parser("log", help="Display history of a given commit.")
argsp.add_argument("commit", default="HEAD", nargs="?", help="Commit to start at.")


# Showing trees
argsp = argsubparsers.add_parser("ls-tree", help="Pretty-print a tree object.")
argsp.add_argument(
    "-r", dest="recursive", action="store_true", help="Recurse into sub-trees"
)

argsp.add_argument("tree", help="Tree object")

# Checkout
argsp = argsubparsers.add_parser(
    "checkout", help="Checkout a commit inside of a directory."
)
argsp.add_argument("commit", help="The commit or tree to checkout.")
argsp.add_argument("path", help="The EMPTY directory to checkout on.")


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


class GitBlob(GitObject):
    fmt = b"blob"

    def serialize(self):
        return self.blobdata

    def deserialize(self, data):
        self.blobdata = data


class GitCommit(GitObject):
    fmt = b"commit"

    def deserialize(self, data):
        self.kvlm = kvlm_parse(data)

    def serialize(self):
        return kvlm_serialize(self.kvlm)

    def init(self):
        self.kvlm = dict()


class GitTag(GitObject):
    fmt = b"tag"

    def serialize(self, repo: GitRepository):
        return super().serialize(repo)

    def deserialize(self, data):
        return super().deserialize(data)

    def init(self):
        return super().init()


class GitTree(GitObject):
    fmt = b"tree"

    def deserialize(self, data):
        self.items = tree_parse(data)

    def serialize(self):
        return tree_serialize(self)

    def init(self):
        self.items = list()


class GitTreeLeaf(object):
    def __init__(self, mode, path, sha) -> None:
        self.mode = mode
        self.path = path
        self.sha = sha


# [mode] space [path] 0x00 [sha-1]
#
# - [mode] is up to six bytes and is an octal representation of a file mode, stored in ASCII. For example, 100644 is encoded with byte values 49 (ASCII “1”), 48 (ASCII “0”), 48, 54, 52, 52. The first two digits encode the file type (file, directory, symlink or submodule), the last four the permissions.
# - It's followed by 0x20, an ASCII space;
# - Followed by the null-terminated (0x00) path;
# - Followed by the object's SHA-1 in binary encoding, on 20 bytes.
def tree_parse_one(raw: str, start: int = 0) -> tuple[int, GitTreeLeaf]:
    # Find the space terminator of the mode
    x = raw.find(b" ", start)
    assert x - start == 5 or x - start == 6

    mode = raw[start:x]
    if len(mode) == 5:
        # Normalize six bytes.
        mode = b" " + mode

    # Read the NULL-terminator to then find the path
    y = raw.find(b"\x00", x)
    path = raw[x + 1 : y]

    # Read the SHA and convert to a hex string
    sha = format(int.from_bytes(raw[y + 1 : y + 21], "big"), "040x")
    return y + 21, GitTreeLeaf(mode, path.decode("utf8"), sha)


def tree_parse(raw: str) -> list:
    pos = 0
    max = len(raw)
    ret = list()
    while pos < max:
        pos, data = tree_parse_one(raw, pos)
        ret.append(data)

    return ret


# Python's default sort doesn't accept a custom comparison function,
# like in most languages, but a `key` arguments that returns a new
# value, which is compared using the default rules. So we just return
# the leaf name, with an extra / if it's a directory, so it will sort the dir after
def tree_leaf_sort_key(leaf: GitTreeLeaf) -> str:
    if leaf.mode.startswith(b"10"):
        return leaf.path

    return leaf.path + "/"


def tree_serialize(obj) -> str:
    obj.items.sort(key=tree_leaf_sort_key)
    ret = b""
    for i in obj.items:
        ret += i.mode
        ret += b" "
        ret += i.path.encode("utf8")
        ret += b"\00x"
        sha = int(i.sha, 16)
        ret += sha.to_bytes(20, byteorder="big")

    return ret


def cmd_ls_tree(args):
    repo = repo_find()
    ls_tree(repo, args.tree, args.recursive)


def ls_tree(repo: GitRepository, ref: str, recurive=None, prefix: str = "") -> None:
    sha = object_find(repo, ref, fmt=b"tree")
    obj = object_read(repo, sha)
    for item in obj.items:
        if len(item.mode) == 5:
            type = item.mode[0:1]  # File
        else:
            type = item.mode[0:2]  # Directory

        match type:
            case b"04":
                type = "tree"
            case b"10":  # Regular file
                type = "blob"
            case b"12":  # A symlink. Blob contents is link target.
                type = "blob"
            case b"16":  # A submodule
                type = "commit"
            case _:
                raise Exception("Weird tree leaf mode {}", format(item.mode))

        if not (recurive and type == "tree"):  # This is a leaf
            print(
                "{0} {1} {2}\t{3}",
                format(
                    "0" * (6 - len(item.mode)) + item.mode.decode("ascii"),
                    type,
                    item.sha,
                    os.path.join(prefix, item.path),
                ),
            )
        else:  # This is a branch, recurse
            ls_tree(repo, item.sha, recurive, os.path.join(prefix, item.path))


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


def cmd_init(args) -> None:
    repo_create(args)


def cmd_cat_file(args) -> None:
    repo = repo_find()
    cat_file(repo, args.object, fmt=args.type.encode())


def cmd_hash_object(args) -> None:
    if args.write:
        repo = repo_find()
    else:
        repo = None

    with open(args.path, "rb") as fd:
        sha = object_hash(fd, args.type.encode(), repo)
        print(sha)


def cmd_log(args):
    repo = repo_find()
    print("dirgraph wyaglob(")
    print("  node[shape=rect]")
    log_graphviz(repo, object_find(repo, args.commit), set())
    print("}")


def log_graphviz(repo: GitRepository, sha, seen: set):
    if sha in seen:
        return

    seen.add(sha)

    commit = object_read(repo, sha)
    message = commit.kvlm[None].decode("utf8").strip()
    message = message.replace("\\", "\\\\")
    message = message.replace('"', '\\"')

    if "\n" in message:  # Keep only the first line
        message = message[: message.index("\n")]

    print('  c_{0} [label="{1}: {2}"]'.format(sha, sha[0:7], message))
    assert commit.fmt == b"commit"

    if not b"parent" in commit.kvlm.keys():
        # Base case: the initial commit.
        return

    parents = commit.kvlm[b"parent"]

    if type(parents) != list:
        parents = [parents]

    for p in parents:
        p = p.decode("ascii")
        print("  c_{0} -> c_{1};".format(sha, p))
        log_graphviz(repo, p, seen)


def cat_file(repo: GitRepository, object: GitObject, fmt=None) -> None:
    object = object_read(repo, object_find(repo, object, fmt=fmt))
    sys.stdout.buffer.write(object.serialize())


def object_find(repo: GitRepository, name: str, fmt=None, follow=True) -> str:
    return name


def object_hash(fd, fmt, repo: GitRepository = None) -> str:
    data = fd.read()

    match fmt:
        case b"commit":
            object = GitCommit(data)
        case b"tree":
            object = GitTree(data)
        case b"tag":
            object = GitTag(data)
        case b"blob":
            object = GitBlob(data)
        case _:
            raise Exception("Unkwown type %s" % fmt)

    return object_write(object, repo)


# Find the root of current repository (where's the file .git is present)
def repo_find(path=".", required=True) -> GitRepository:
    path = os.path.realpath(path)

    # Check if parent contains the .git directory
    if os.path.isdir(os.path.join(path, ".git")):
        return GitRepository(path)

    # If we haven't returned, try to get parent directory
    parent = os.path.relpath(os.path.join(path, ".."))

    if parent == path:
        # If parent == path, then path is root
        if required:
            raise Exception("No git directory found.")
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


# Key-Value List with Message (commits and tags parser)
def kvlm_parse(raw: str, start: int = 0, dct: dict = None) -> dict:
    if not dct:
        dct = collections.OrderedDict()

    # This function is recursive: it reads a key/value pair, then call
    # itself back with the new position.

    space = raw.find(b" ", start)
    new_line = raw.find(b"\n", start)

    # If space appears before newline, we have a kayword. Otherwise,
    # it's the final message, which we just read to the end of the file.

    # Base case
    # =========
    # If newline appears first (or there's no space at all, in which
    # case find returns -1), we assume a blank line.  A blank line
    # means the remainder of the data is the message.  We store it in
    # the dictionary, with None as the key, and return.
    if (space < 0) or (new_line < space):
        assert new_line == start
        dct[None] = raw[start + 1 :]
        return dct

    # Recursive case
    # =========
    # We read a key-value pair and recurse for the next.
    key = raw[start:space]

    # Find the end of the value. Continuation lines begin with a
    # space, so we loop until we find a "\n" not followed by a space.
    end = start
    while True:
        end = raw.find(b"\n", end + 1)
        if raw[end + 1] != ord(" "):
            break

    # Grab value and drop the leading space on continuation lines.
    value = raw[space + 1 : end].replace(b"\n ", b"\n")

    if key in dct:
        if type(dct[key]) == list:
            dct[key].append(value)
        else:
            dct[key] = [dct[key], value]
    else:
        dct[key] = value

    return kvlm_parse(raw, start=end + 1, dct=dct)


def kvlm_serialize(kvlm: dict) -> str:
    ret = b""

    for key in kvlm.keys():
        # Skip the message itself
        if key == None:
            continue
        val = kvlm[key]
        # Normalize to a list
        if type(val) != list:
            val = [val]

        for v in val:
            ret += key + b" " + (v.replace(b"\n", b"\n ")) + b"\n"

    ret += b"\n" + kvlm[None] + b"\n"

    return ret


def cmd_checkout(args) -> None:
    repo = repo_find()

    sha = object_find(repo, args.commit)
    obj = object_read(repo, sha)

    # If the object is a commit, grab its tree
    if obj.fmt == b"commit":
        obj = object_read(repo, obj.kvlm[b"tree"].decode("ascii"))

    if os.path.exists(args.path):
        if not os.path.isdir(args.path):
            raise Exception("Not a directory {0}.".format(args.path))
        if not os.listdir(args.path):
            raise Exception("Not empty {0}.".format(args.path))
    else:
        os.makedirs(args.path)

    tree_checkout(repo, obj, os.path.realpath(args.path))


def tree_checkout(repo: GitRepository, tree: GitTree, path: str) -> None:
    for item in tree.items:
        obj = object_read(repo, item.sha)
        dest = os.path.join(repo, item.sha)

        if obj.fmt == b"tree":
            os.mkdir(dest)
            tree_checkout(repo, obj, dest)
        elif obj.fmt == b"blob":
            with open(dest, "wb") as f:
                f.write(obj.blobdata)
