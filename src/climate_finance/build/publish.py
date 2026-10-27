"""Publishing a packed release where `cf.load()` finds it.

Every pack and the release index, `releases.json`, are assets of one GitHub release
tagged `DATA_TAG`, from which `climate_finance.release.INDEX_URL` reads the index. A
fixed tag keeps the index off GitHub's "latest" release, which follows code
releases. `publish_release` works through `gh`, the GitHub CLI, which holds the
maintainer's credentials. Every `gh` call takes an argument list and runs no
shell, and its arguments are constants, absolute paths, a repository checked
against `_REPO`, and asset API paths checked against `_ASSET_API`.
"""

from __future__ import annotations

import json
import re
import shlex
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from pydantic import ValidationError

from climate_finance.build.errors import BuildError
from climate_finance.build.release import publishable
from climate_finance.release import IndexEntry, ReleaseIndex, release_dirname
from climate_finance.schema import ReleaseManifest, sha256_file

# The repository and release tag whose assets are every pack and the index.
PUBLISH_REPO = "ONEcampaign/climate-finance-package"
DATA_TAG = "data"
INDEX_ASSET = "releases.json"
# The new index goes up under this name, then takes `INDEX_ASSET`'s place.
STAGED_INDEX_ASSET = "releases.next.json"
# The directory beside a pack that holds the index a publish is writing until
# GitHub holds it (`recovery_index`).
RECOVERY_DIR = "publish-recovery"
# A published release name: a year, a letter, and a revision after a re-cut, such
# as 2026a or 2026a.1. Candidates and test builds (2026a-rc) never match.
PUBLISHED_NAME = re.compile(r"(\d{4})([a-z])(?:\.(\d+))?")
# Seconds a `gh` call may run: a 400 MB pack upload at 1 Mbit/s fits.
GH_TIMEOUT = 3600
_REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
_ASSET_API = re.compile(
    r"https://api\.github\.com/(repos/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+"
    r"/releases/assets/\d+)"
)
_DATA_TITLE = "Data releases"
_DATA_NOTES = (
    "Packed climate-finance releases, and releases.json, the index cf.load() reads. "
    "Publish with `cf-build publish`."
)

# Runs `gh` with the given arguments and returns its standard output.
type Gh = Callable[[Sequence[str]], str]


def run_gh(args: Sequence[str]) -> str:
    """The standard output of `gh` run with `args`, within `GH_TIMEOUT` seconds.

    Raises:
        BuildError: when `gh` is not installed, exits with an error or runs out
            of time.
    """
    command = shlex.join(args)
    try:
        done = subprocess.run(
            ["gh", *args],
            capture_output=True,
            text=True,
            check=True,
            timeout=GH_TIMEOUT,
        )
    except FileNotFoundError as error:
        raise BuildError(
            "publishing needs the GitHub CLI, gh, logged in with `gh auth login`"
        ) from error
    except subprocess.TimeoutExpired as error:
        raise BuildError(
            f"`gh {command}` ran for over {GH_TIMEOUT} seconds and was stopped"
        ) from error
    except subprocess.CalledProcessError as error:
        raise BuildError(f"`gh {command}` failed: {error.stderr.strip()}") from error
    return done.stdout


def asset_url(name: str, *, repo: str = PUBLISH_REPO) -> str:
    """The download URL of the asset `name` on `repo`'s `DATA_TAG` release."""
    return f"https://github.com/{repo}/releases/download/{DATA_TAG}/{quote(name)}"


@dataclass(frozen=True)
class Publication:
    """A published release, or under a dry run one ready to publish.

    `index` is the new `releases.json`: beside the pack once published, in a
    temporary directory under a dry run. `commands` are the `gh` arguments that
    change the GitHub release, in the order they run.
    """

    release: str
    url: str
    sha256: str
    latest: str
    index: Path
    commands: tuple[tuple[str, ...], ...]
    dry_run: bool


def publish_release(
    pack: Path,
    *,
    repo: str = PUBLISH_REPO,
    gh: Gh = run_gh,
    dry_run: bool = False,
) -> Publication:
    """Upload `pack` to `repo`'s `DATA_TAG` release and replace the index with one
    that lists it.

    Every check, the sha256 and the upload read one copy of `pack` in a private
    temporary directory. The current index is read from the release, or starts
    empty while the release does not exist. Before GitHub changes, the new index is
    written to `recovery_index(pack)`. The release is then created, published and
    not marked latest. The pack goes up first. The new index goes up as
    `STAGED_INDEX_ASSET` and then takes the old one's place by a delete and a
    rename, so `releases.json` is missing only between those two API calls. The
    index is then read back and must equal the one written. The new index keeps
    every field of the old one, those this package does not know included. Once
    read back, it moves from the recovery directory to `releases.json` beside
    `pack`, the file to ship in the wheel's `data/` at the next package release.
    Under `dry_run` nothing on GitHub or beside the pack changes, and the new index
    goes to a temporary file.

    Raises:
        GateFailedError: when the pack's gates keep it from publishing
            (`release.publishable`).
        BuildError: when the pack's settings keep it from publishing
            (`release.publishable`), its release name is not a published name
            (`PUBLISHED_NAME`), its file is named for another release, `repo` is
            not `owner/name`, the release holds packs but no index, the index
            refuses it (`add_to_index`), the index read back differs from the one
            written, the `DATA_TAG` release is a draft, or `gh` fails.
    """
    if not _REPO.fullmatch(repo):
        raise BuildError(f"--repo {repo!r} is not a GitHub repository, owner/name")
    recovery = recovery_index(pack)
    with tempfile.TemporaryDirectory(prefix="publish-") as scratch:
        # Every check, the sha256 and the upload read one private copy, so a pack
        # replaced at `pack` meanwhile is never published unchecked.
        snapshot = Path(scratch) / pack.name
        shutil.copyfile(pack, snapshot)
        manifest = publishable(snapshot)
        release_order(manifest.release)
        # The asset takes the file's name, so a renamed pack could replace another
        # release's.
        if pack.name != (expected := f"{release_dirname(manifest.release)}.tar.zst"):
            raise BuildError(
                f"{pack} holds release {manifest.release!r}; publish it as {expected}"
            )
        where = ("--repo", repo)
        assets = _data_assets(gh, repo=repo)
        index: dict[str, Any] = {}
        if assets is not None:
            if INDEX_ASSET in assets:
                index = _read_index(gh, repo=repo)
            elif any(name.endswith(".tar.zst") for name in assets):
                raise BuildError(_missing_index(repo, recovery=recovery))
        sha256 = sha256_file(snapshot)
        url = asset_url(pack.name, repo=repo)
        updated = add_to_index(index, manifest=manifest, sha256=sha256, url=url)
        rendered = json.dumps(updated, indent=2) + "\n"
        staged = Path(scratch) / STAGED_INDEX_ASSET
        staged.write_text(rendered)
        create = (
            "release",
            "create",
            DATA_TAG,
            "--title",
            _DATA_TITLE,
            "--notes",
            _DATA_NOTES,
            "--latest=false",
            *where,
        )
        # A pack asset the index does not name is left from a publish that stopped
        # before replacing the index, so it is replaced.
        uploads = (
            *([create] if assets is None else []),
            ("release", "upload", DATA_TAG, str(snapshot), "--clobber", *where),
            ("release", "upload", DATA_TAG, str(staged), "--clobber", *where),
        )
        delete = ("release", "delete-asset", DATA_TAG, INDEX_ASSET, "--yes", *where)
        swap = (delete,) if INDEX_ASSET in (assets or ()) else ()
        if dry_run:
            preview = Path(tempfile.mkdtemp()) / INDEX_ASSET
            preview.write_text(rendered)
            placeholder = f"repos/{repo}/releases/assets/<{STAGED_INDEX_ASSET}>"
            return Publication(
                release=manifest.release,
                url=url,
                sha256=sha256,
                latest=updated["latest"],
                index=preview,
                commands=(*uploads, *swap, _rename(placeholder)),
                dry_run=True,
            )
        # Written before GitHub changes, so a publish that stops leaves the index
        # that completes it where `_missing_index` names it.
        recovery.parent.mkdir(exist_ok=True)
        recovery.write_text(rendered)
        for command in uploads:
            gh(command)
    rename = _rename(_asset_api_path(gh, repo=repo, name=STAGED_INDEX_ASSET))
    for command in (*swap, rename):
        gh(command)
    if _read_index(gh, repo=repo) != updated:
        raise BuildError(
            f"the {INDEX_ASSET} read back from {repo} differs from the one this "
            f"publish wrote, so another publish replaced it meanwhile; publish "
            f"{manifest.release} again, which adds it to the index that is there now"
        )
    index_path = pack.with_name(INDEX_ASSET)
    recovery.replace(index_path)
    recovery.parent.rmdir()
    return Publication(
        release=manifest.release,
        url=url,
        sha256=sha256,
        latest=updated["latest"],
        index=index_path,
        commands=(*uploads, *swap, rename),
        dry_run=False,
    )


def recovery_index(pack: Path) -> Path:
    """Where publishing `pack` keeps the index it is writing until GitHub holds it:
    `publish-recovery/releases.json` beside the pack, named so that
    `gh release upload` attaches it as the index."""
    return pack.with_name(RECOVERY_DIR) / INDEX_ASSET


def _missing_index(repo: str, *, recovery: Path) -> str:
    """Why a `DATA_TAG` release with packs and no index is refused, and the index
    that restores it."""
    stopped = (
        f"the {DATA_TAG!r} release of {repo} holds packs but no {INDEX_ASSET}, as "
        "after a publish that stopped before its index was in place"
    )
    if recovery.is_file():
        return (
            f"{stopped}. The index that publish was writing is {recovery}; upload "
            f"it with `gh release upload {DATA_TAG} {recovery} --repo {repo}`, "
            "then publish again"
        )
    return (
        f"{stopped}. No publish of a pack in this directory left an index at "
        f"{recovery}; upload the publish-recovery/{INDEX_ASSET} of the publish "
        f"that stopped, from beside its pack, with `gh release upload {DATA_TAG} "
        f"<path> --repo {repo}`, then publish again"
    )


def _rename(api_path: str) -> tuple[str, ...]:
    return ("api", "--method", "PATCH", api_path, "-f", f"name={INDEX_ASSET}")


def _data_assets(gh: Gh, *, repo: str) -> set[str] | None:
    """The asset names of `repo`'s `DATA_TAG` release, or None while it has none.

    Raises:
        BuildError: when the release is a draft, whose assets only maintainers can
            download.
    """
    where = ("--repo", repo)
    listed = json.loads(
        gh(["release", "list", "--json", "tagName,isDraft", "--limit", "1000", *where])
    )
    data = next((row for row in listed if row["tagName"] == DATA_TAG), None)
    if data is None:
        return None
    if data["isDraft"]:
        raise BuildError(
            f"the {DATA_TAG!r} release of {repo} is a draft, whose assets only "
            "maintainers can download; publish it on GitHub, then publish again"
        )
    viewed = json.loads(gh(["release", "view", DATA_TAG, "--json", "assets", *where]))
    return {asset["name"] for asset in viewed["assets"]}


def _asset_api_path(gh: Gh, *, repo: str, name: str) -> str:
    """The API path of the asset `name` on `repo`'s `DATA_TAG` release.

    Raises:
        BuildError: when the release has no such asset, or GitHub gives its API
            URL in an unexpected form.
    """
    viewed = json.loads(
        gh(["release", "view", DATA_TAG, "--json", "assets", "--repo", repo])
    )
    urls = [asset["apiUrl"] for asset in viewed["assets"] if asset["name"] == name]
    if len(urls) != 1 or not (matched := _ASSET_API.fullmatch(urls[0])):
        raise BuildError(f"the {DATA_TAG!r} release of {repo} has no asset {name}")
    return matched[1]


def _read_index(gh: Gh, *, repo: str) -> dict[str, Any]:
    """The `releases.json` on `repo`'s `DATA_TAG` release.

    Raises:
        BuildError: when it is not JSON.
    """
    downloaded = gh(
        [
            "release",
            "download",
            DATA_TAG,
            "--pattern",
            INDEX_ASSET,
            "--output",
            "-",
            "--repo",
            repo,
        ]
    )
    try:
        return json.loads(downloaded)
    except json.JSONDecodeError as error:
        raise BuildError(f"the published {INDEX_ASSET} is not JSON") from error


def add_to_index(
    index: dict[str, Any], *, manifest: ReleaseManifest, sha256: str, url: str
) -> dict[str, Any]:
    """`index`, a parsed `releases.json`, with `manifest`'s release published at
    `url` with the pack sha256 `sha256`.

    The release becomes `latest` only when it follows the current latest in
    `release_order`, so republishing an older release leaves `latest` alone.
    Fields of `index` this package does not know are kept.

    Raises:
        BuildError: when `index` is not a release index or its latest is not a
            published name, the index already lists the release, or the drift
            gate compared the release with a release other than the one it must:
            the current latest for a new latest, or any listed release for an
            older one, in each case the pack the index lists under that name.
    """
    try:
        known = ReleaseIndex.model_validate(index)
    except ValidationError as error:
        raise BuildError(
            f"the published {INDEX_ASSET} is not an index this package reads: {error}"
        ) from error
    release, previous = manifest.release, manifest.previous_release
    if (listed := known.releases.get(release)) is not None:
        raise BuildError(
            f"release {release!r} is already published, as the pack with sha256 "
            f"{listed.sha256}; a name names one pack, so cut the release again "
            f"under a new name, such as {_next_revision(release)}"
        )
    newest = known.latest is None or release_order(release) > release_order(
        known.latest
    )
    if newest and previous != known.latest:
        expected = (
            "--first-release, as the index lists no release"
            if known.latest is None
            else f"--previous naming {known.latest!r}, the latest published release"
        )
        raise BuildError(
            f"the drift gate compared {release!r} with "
            f"{'nothing' if previous is None else repr(previous)}; cut it again "
            f"with {expected}"
        )
    if not newest and previous not in known.releases:
        raise BuildError(
            f"the drift gate compared {release!r} with {previous!r}, which the "
            "index does not list; cut it again with --previous naming a published "
            "release"
        )
    if previous is not None:
        published = known.releases[previous].sha256
        if manifest.previous_pack_sha256 != published:
            copy = (
                "a copy that records no pack"
                if manifest.previous_pack_sha256 is None
                else f"a copy from the pack with sha256 {manifest.previous_pack_sha256}"
            )
            raise BuildError(
                f"the drift gate compared {release!r} with {copy} of {previous!r}, "
                f"and the index lists {previous!r} as the pack with sha256 "
                f"{published}; cut it again with --previous naming the published "
                f"release, as cf.load({previous!r}, refresh=True).path holds it"
            )
    entry = IndexEntry(url=url, sha256=sha256, data_format=manifest.data_format)
    return {
        **index,
        "latest": release if newest else known.latest,
        "releases": {**index.get("releases", {}), release: entry.model_dump()},
    }


def release_order(release: str) -> tuple[int, str, int]:
    """The sort key of a published release name (`PUBLISHED_NAME`): its year, its
    letter, then its revision, so 2026a.10 follows 2026a.9 and 2026a.1 follows
    2026a.

    Raises:
        BuildError: when `release` is not a published name.
    """
    if not (matched := PUBLISHED_NAME.fullmatch(release)):
        raise BuildError(
            f"release name {release!r} is not a published name: a year, a "
            "lowercase letter, and a revision after a re-cut, such as 2026a or "
            "2026a.1"
        )
    year, letter, revision = matched.groups()
    return int(year), letter, int(revision or 0)


def _next_revision(release: str) -> str:
    year, letter, revision = release_order(release)
    return f"{year}{letter}.{revision + 1}"
