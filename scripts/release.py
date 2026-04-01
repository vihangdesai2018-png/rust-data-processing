#!/usr/bin/env python3
"""
Interactive release: show last git tag and current versions, prompt for new SemVer,
bump Cargo.toml / pyproject / lockfiles / CHANGELOG, commit, push main, annotated tag, push tag.

Run from repo root:
  python scripts/release.py
  ./scripts/release_tag.ps1

Requires: Python 3.10+, git on PATH.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO_SLUG = "vihangdesai2018-png/rust-data-processing"
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$")

BUMP_PATHS = [
    "Cargo.toml",
    "Cargo.lock",
    "CHANGELOG.md",
    "python-wrapper/Cargo.toml",
    "python-wrapper/pyproject.toml",
    "python-wrapper/Cargo.lock",
    "python-wrapper/uv.lock",
]


def git(*args: str, cwd: Path | None = None) -> str:
    p = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if p.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed ({p.returncode}): {p.stderr.strip()}")
    return p.stdout.strip()


def git_ok(*args: str, cwd: Path | None = None) -> bool:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True).returncode == 0


def normalize_version(raw: str) -> str:
    t = raw.strip()
    if not t:
        raise ValueError("Version must not be empty.")
    if t[0] in "vV":
        t = t[1:]
    if not SEMVER_RE.match(t):
        raise ValueError(f"Not SemVer-like: {raw!r} (e.g. 0.2.0)")
    return t


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8", newline="\n")


def get_version_cargo_package(content: str) -> str:
    lines = content.splitlines()
    in_pkg = False
    for line in lines:
        s = line.strip()
        if s == "[package]":
            in_pkg = True
            continue
        if in_pkg:
            if s.startswith("[") and s.endswith("]"):
                break
            m = re.match(r'version\s*=\s*"([^"]+)"', s)
            if m:
                return m.group(1)
    raise ValueError("Cargo.toml: no version= in [package]")


def get_version_pyproject(content: str) -> str:
    lines = content.splitlines()
    in_proj = False
    for line in lines:
        s = line.strip()
        if s == "[project]":
            in_proj = True
            continue
        if s.startswith("[") and s != "[project]":
            in_proj = False
        if in_proj:
            m = re.match(r'version\s*=\s*"([^"]+)"', s)
            if m:
                return m.group(1)
    raise ValueError("pyproject.toml: no version in [project]")


def read_aligned_versions(repo: Path) -> tuple[str, str, str]:
    root = get_version_cargo_package(read_text(repo / "Cargo.toml"))
    py_cargo = get_version_cargo_package(read_text(repo / "python-wrapper" / "Cargo.toml"))
    pyproj = get_version_pyproject(read_text(repo / "python-wrapper" / "pyproject.toml"))
    return root, py_cargo, pyproj


def set_version_in_cargo_package(content: str, new_ver: str) -> str:
    lines = content.splitlines(keepends=True)
    out: list[str] = []
    in_package = False
    replaced = False
    for line in lines:
        s = line.strip()
        if s.startswith("[") and s.endswith("]"):
            if s == "[package]":
                in_package = True
            else:
                in_package = False
            out.append(line)
            continue
        if in_package and not replaced and re.match(r"^\s*version\s*=\s*\"", line):
            out.append(f'version = "{new_ver}"\n')
            replaced = True
            continue
        out.append(line)
    if not replaced:
        raise ValueError("Cargo.toml: no version= in [package]")
    return "".join(out)


def set_version_in_pyproject_project(content: str, new_ver: str) -> str:
    lines = content.splitlines(keepends=True)
    out: list[str] = []
    in_project = False
    replaced = False
    for line in lines:
        s = line.strip()
        if s == "[project]":
            in_project = True
            out.append(line)
            continue
        if s.startswith("[") and s != "[project]":
            in_project = False
        if in_project and not replaced and re.match(r"^\s*version\s*=\s*\"", line):
            out.append(f'version = "{new_ver}"\n')
            replaced = True
            continue
        out.append(line)
    if not replaced:
        raise ValueError("pyproject.toml: no version in [project]")
    return "".join(out)


def bump_cargo_lock_named_packages(content: str, names: set[str], new_ver: str) -> str:
    lines = content.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    while i < len(lines):
        if lines[i].strip() != "[[package]]":
            out.append(lines[i])
            i += 1
            continue
        out.append(lines[i])
        i += 1
        pkg_name: str | None = None
        replaced = False
        while i < len(lines) and lines[i].strip() != "[[package]]":
            m = re.match(r'\s*name\s*=\s*"([^"]+)"', lines[i])
            if m:
                pkg_name = m.group(1)
            if (
                pkg_name in names
                and not replaced
                and re.match(r'^\s*version\s*=\s*"[^"]*"', lines[i])
            ):
                out.append(f'version = "{new_ver}"\n')
                replaced = True
            else:
                out.append(lines[i])
            i += 1
        if pkg_name in names and not replaced:
            raise ValueError(f"Cargo.lock: no version line for package {pkg_name!r}")
    return "".join(out)


def bump_uv_lock_rust_data_processing(content: str, new_ver: str) -> str:
    lines = content.splitlines(keepends=True)
    out: list[str] = []
    i = 0
    while i < len(lines):
        if lines[i].strip() != "[[package]]":
            out.append(lines[i])
            i += 1
            continue
        out.append(lines[i])
        i += 1
        target = False
        replaced = False
        while i < len(lines) and lines[i].strip() != "[[package]]":
            m = re.match(r'\s*name\s*=\s*"([^"]+)"', lines[i])
            if m and m.group(1) == "rust-data-processing":
                target = True
            if (
                target
                and not replaced
                and re.match(r'^\s*version\s*=\s*"[^"]*"', lines[i])
            ):
                out.append(f'version = "{new_ver}"\n')
                replaced = True
            else:
                out.append(lines[i])
            i += 1
        if target and not replaced:
            raise ValueError("uv.lock: no version for rust-data-processing")
    return "".join(out)


def insert_changelog_section(content: str, ver: str, today: str) -> str:
    block = f"## [{ver}] - {today}\n\n### Changed\n\n- (summarize this release)\n\n"
    m = re.search(r"^## \[\d+\.\d+\.\d+\]", content, re.M)
    if not m:
        raise ValueError("CHANGELOG.md: no ## [x.y.z] section found")
    return content[: m.start()] + block + content[m.start() :]


def insert_changelog_link(content: str, ver: str) -> str:
    link = f"[{ver}]: https://github.com/{REPO_SLUG}/releases/tag/v{ver}\n"
    m = re.search(r"^\[\d+\.\d+\.\d+\]: https://github.com", content, re.M)
    if m:
        return content[: m.start()] + link + content[m.start() :]
    return content.rstrip() + "\n" + link


def get_last_tag(repo: Path) -> str | None:
    try:
        out = git("tag", "-l", "v*", "--sort=-version:refname", cwd=repo)
    except RuntimeError:
        return None
    if not out:
        return None
    first = out.splitlines()[0].strip()
    return first or None


def run_bump(repo: Path, new_ver: str, today: str) -> None:
    p = repo / "Cargo.toml"
    write_text(p, set_version_in_cargo_package(read_text(p), new_ver))

    p = repo / "python-wrapper" / "Cargo.toml"
    write_text(p, set_version_in_cargo_package(read_text(p), new_ver))

    p = repo / "python-wrapper" / "pyproject.toml"
    write_text(p, set_version_in_pyproject_project(read_text(p), new_ver))

    p = repo / "Cargo.lock"
    write_text(
        p,
        bump_cargo_lock_named_packages(read_text(p), {"rust-data-processing"}, new_ver),
    )

    p = repo / "python-wrapper" / "Cargo.lock"
    write_text(
        p,
        bump_cargo_lock_named_packages(
            read_text(p),
            {"rust-data-processing", "rust_data_processing_py"},
            new_ver,
        ),
    )

    p = repo / "python-wrapper" / "uv.lock"
    write_text(p, bump_uv_lock_rust_data_processing(read_text(p), new_ver))

    p = repo / "CHANGELOG.md"
    text = read_text(p)
    if f"## [{new_ver}]" in text:
        raise ValueError(f"CHANGELOG.md already has ## [{new_ver}]")
    text = insert_changelog_section(text, new_ver, today)
    text = insert_changelog_link(text, new_ver)
    write_text(p, text)


def assert_clean_tree(repo: Path, allow_dirty: bool) -> None:
    st = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    if st.strip() and not allow_dirty:
        raise RuntimeError(
            "Working tree is dirty. Commit or stash, or pass --allow-dirty.\n" + st
        )


def assert_on_branch(repo: Path, main_branch: str) -> None:
    b = git("branch", "--show-current", cwd=repo)
    if b != main_branch:
        raise RuntimeError(f"Must be on branch {main_branch!r} (currently {b!r}).")


def git_commit_bump(repo: Path, new_ver: str) -> None:
    subprocess.run(["git", "add", *BUMP_PATHS], cwd=repo, check=True)
    subprocess.run(
        ["git", "commit", "-m", f"chore: release v{new_ver}"],
        cwd=repo,
        check=True,
    )


def sync_and_match_remote(repo: Path, remote: str, main_branch: str) -> None:
    git("fetch", remote, main_branch, "--tags", cwd=repo)
    git("pull", "--ff-only", remote, main_branch, cwd=repo)
    upstream = f"{remote}/{main_branch}"
    head = git("rev-parse", "HEAD", cwd=repo)
    main_sha = git("rev-parse", upstream, cwd=repo)
    if head != main_sha:
        raise RuntimeError(
            f"HEAD does not match {upstream} after pull.\n"
            f"  Local:  {head}\n  Remote: {main_sha}\n"
            "Push local main if you are ahead, or retry."
        )


def assert_tag_available(repo: Path, remote: str, tag_name: str) -> None:
    if git("tag", "-l", tag_name, cwd=repo):
        raise RuntimeError(
            f"Tag {tag_name!r} exists locally. Delete only if not pushed: git tag -d {tag_name}"
        )
    ref = f"refs/tags/{tag_name}"
    out = subprocess.run(
        ["git", "ls-remote", remote, ref],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    if out.strip():
        raise RuntimeError(f"Tag {tag_name!r} already exists on {remote}.")


def do_push_main(repo: Path, remote: str, main_branch: str) -> None:
    git("push", remote, main_branch, cwd=repo)


def do_tag_push(repo: Path, remote: str, tag_name: str, message: str) -> None:
    git("tag", "-a", tag_name, "-m", message, cwd=repo)
    git("push", remote, tag_name, cwd=repo)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Bump monorepo version, CHANGELOG, commit, push main, tag vX.Y.Z, push tag."
    )
    ap.add_argument("version", nargs="?", help="New SemVer (e.g. 0.1.3). Omit to be prompted.")
    ap.add_argument("--remote", default="origin")
    ap.add_argument("--branch", default="main", dest="main_branch")
    ap.add_argument("--comment", help="Annotated tag message (use with -y for non-interactive).")
    ap.add_argument("--allow-dirty", action="store_true", help="Allow dirty tree before bump.")
    ap.add_argument(
        "--no-commit",
        action="store_true",
        help="Only write files; do not commit, push, or tag.",
    )
    ap.add_argument(
        "--skip-git",
        action="store_true",
        help="Only bump files; no commit, push, or tag.",
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-y", "--yes", action="store_true", help="Skip confirmation prompts.")
    args = ap.parse_args()

    repo = Path(__file__).resolve().parent.parent
    today = date.today().isoformat()

    # --- preflight ---
    if not git_ok("rev-parse", "--git-dir", cwd=repo):
        print("Not a git repository.", file=sys.stderr)
        return 1

    try:
        git("fetch", args.remote, args.main_branch, "--tags", cwd=repo)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    try:
        assert_clean_tree(repo, args.allow_dirty)
        assert_on_branch(repo, args.main_branch)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    last_tag = get_last_tag(repo)
    try:
        rv, pv, qv = read_aligned_versions(repo)
    except (ValueError, OSError) as e:
        print(e, file=sys.stderr)
        return 1

    if rv != pv or rv != qv:
        print(
            f"Version mismatch in repo: root={rv!r} python-wrapper/Cargo.toml={pv!r} pyproject={qv!r}. "
            "Align manually, then retry.",
            file=sys.stderr,
        )
        return 1

    print("== Last release tag (v*, after fetch) ==")
    print(f"    {last_tag or '(none)'}")
    print("== Current package versions (must match) ==")
    print(f"    Cargo.toml (root):              {rv}")
    print(f"    python-wrapper/Cargo.toml:      {pv}")
    print(f"    python-wrapper/pyproject.toml:  {qv}")
    print()

    if args.version:
        try:
            new_ver = normalize_version(args.version)
        except ValueError as e:
            print(e, file=sys.stderr)
            return 1
    else:
        if not args.yes:
            a = input("Proceed to enter the new version? [y/N]: ").strip().lower()
            if a not in ("y", "yes"):
                print("Aborted.")
                return 0
        raw = input("New SemVer (e.g. 0.1.3): ").strip()
        try:
            new_ver = normalize_version(raw)
        except ValueError as e:
            print(e, file=sys.stderr)
            return 1

    if new_ver == rv:
        print(
            f"New version {new_ver} matches root Cargo.toml. Choose a higher version.",
            file=sys.stderr,
        )
        return 1

    tag_name = f"v{new_ver}"
    print("\nWill update:")
    for rel in BUMP_PATHS:
        print(f"  - {rel}")

    if not args.yes and not args.dry_run:
        a = input(f"\nWrite {new_ver} to these files and CHANGELOG? [y/N]: ").strip().lower()
        if a not in ("y", "yes"):
            print("Aborted.")
            return 0

    if args.dry_run:
        print(f"[dry-run] would bump to {new_ver}, commit, tag {tag_name}")
        return 0

    try:
        run_bump(repo, new_ver, today)
    except (ValueError, OSError) as e:
        print(e, file=sys.stderr)
        return 1

    rv2, _, _ = read_aligned_versions(repo)
    if rv2 != new_ver:
        print(f"Internal error: expected {new_ver}, got {rv2}", file=sys.stderr)
        return 1

    if args.skip_git:
        print("Done (--skip-git). Commit, push main, then tag when ready.")
        return 0

    if args.no_commit:
        print("Files written. Commit with: git add ... && git commit -m 'chore: release v%s'" % new_ver)
        return 0

    try:
        git_commit_bump(repo, new_ver)
    except subprocess.CalledProcessError as e:
        print(e, file=sys.stderr)
        return 1

    msg: str | None = args.comment
    if not msg:
        if args.yes:
            print("Error: use --comment with -y for the annotated tag message.", file=sys.stderr)
            return 1
        hint = f"previous tag: {last_tag}" if last_tag else "no previous tag"
        msg = input(f"Annotated tag message for {tag_name} ({hint}): ").strip()
    if not msg:
        print("Tag message must not be empty.", file=sys.stderr)
        return 1

    if not args.yes:
        a = input(
            f"\nPush {args.main_branch} to {args.remote}, then create tag {tag_name} and push? [y/N]: "
        ).strip().lower()
        if a not in ("y", "yes"):
            print("Aborted after commit. Reset with git reset --hard HEAD~1 if needed.")
            return 0

    try:
        do_push_main(repo, args.remote, args.main_branch)
        sync_and_match_remote(repo, args.remote, args.main_branch)
        assert_tag_available(repo, args.remote, tag_name)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    try:
        do_tag_push(repo, args.remote, tag_name, msg)
    except RuntimeError as e:
        print(e, file=sys.stderr)
        return 1

    print(f"\nDone. Pushed tag {tag_name}. CI: rust_release.yml and python_release.yml.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
