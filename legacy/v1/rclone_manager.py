import os
import re
import json
import logging
import asyncio
import unicodedata
from pathlib import Path
try:
    from .config import Config
except ImportError:  # pragma: no cover - direct script compatibility
    from config import Config

class RcloneManager:
    @staticmethod
    def _rclone_filter_escape(name: str) -> str:
        """Escape one path segment for rclone include filters."""
        return re.sub(r"([\\*?\[\]{}])", r"\\\1", name)

    @staticmethod
    def _rclone_is_missing_or_dir_error(text: str) -> bool:
        lowered = (text or "").lower()
        return (
            "doesn't exist" in lowered
            or "does not exist" in lowered
            or "is a directory" in lowered
            or "directory not found" in lowered
            or "object not found" in lowered
        )

    @staticmethod
    def _rclone_is_auth_error(text: str) -> bool:
        """rclone/OneDrive 인증 오류 여부."""
        lowered = (text or "").lower()
        return any(
            marker in lowered
            for marker in (
                "unauthenticated",
                "invalidauthenticationtoken",
                "access token has expired",
                "token expired",
                "refresh token",
                "unauthorized",
            )
        )

    @staticmethod
    def _normalize_filename_for_match(name: str) -> str:
        """파일명 비교용 정규화: NFC + 모든 따옴표/특수문자/공백을 _로 통일"""
        n = unicodedata.normalize("NFC", name)
        n = re.sub(r'[\u2018\u2019\u201c\u201d\u0022\u0027\u0060\u00b4\uff07]', '_', n)
        n = re.sub(r'[\s\/:*?<>|,.!@#$%^&ⓒ;()\[\]]+', '_', n)
        n = n.strip('_').lower()
        return n

    def _find_remote_filename(self, local_fname: str, remote_files: dict[str, int]) -> str | None:
        if local_fname in remote_files:
            return local_fname
        norm_local = self._normalize_filename_for_match(local_fname)
        for rname in remote_files:
            if self._normalize_filename_for_match(rname) == norm_local:
                return rname
        local_lower = local_fname.lower()
        for rname in remote_files:
            if local_lower in rname.lower() or rname.lower() in local_lower:
                return rname
        return None

    async def _rclone_cleanup(self):
        logging.info("Running rclone cleanup on remote...")
        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        proc = await asyncio.create_subprocess_exec(
            Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG,
            "cleanup", Config.RCLONE_REMOTE, env=rclone_env,
        )
        await proc.wait()
        return proc.returncode == 0

    async def _rclone_delete_remote(
        self,
        remote_path: str,
        remote_dir: str | None = None,
        filename: str | None = None,
    ) -> tuple[bool, str]:
        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        proc = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "deletefile", remote_path, env=rclone_env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        _, stderr = await proc.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode == 0 or not remote_dir or not filename:
            return proc.returncode == 0, stderr_text
        if self._rclone_is_auth_error(stderr_text) or not self._rclone_is_missing_or_dir_error(stderr_text):
            return False, stderr_text

        include_filter = f"/{self._rclone_filter_escape(filename)}"
        logging.warning("deletefile could not address %s directly; trying filtered delete in parent dir with include=%s", remote_path, include_filter)
        fallback = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "delete", remote_dir, "--max-depth", "1", "--include", include_filter, env=rclone_env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        _, fallback_stderr = await fallback.communicate()
        fallback_err = fallback_stderr.decode("utf-8", errors="replace").strip()
        if fallback.returncode != 0:
            return False, f"{stderr_text}\nfiltered delete: {fallback_err}".strip()

        remote_files, list_err = await self._rclone_lsjson_dir(remote_dir)
        if list_err:
            return False, f"{stderr_text}\nfiltered delete verify: {list_err}".strip()
        if filename in remote_files:
            return False, (f"{stderr_text}\nfiltered delete returned ok but file still exists (size={remote_files[filename]})")

        logging.info("filtered delete removed stale remote file: %s/%s", remote_dir, filename)
        return True, f"{stderr_text}\nfiltered delete ok".strip()

    async def _rclone_stat_remote(self, remote_path: str) -> tuple[int | None, str]:
        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        proc = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "lsjson", remote_path, env=rclone_env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        out, stderr = await proc.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode != 0:
            return None, stderr_text
        if not out:
            return None, ""
        try:
            items = json.loads(out.decode())
            if items and len(items) > 0:
                return items[0].get("Size", 0), ""
            return None, ""
        except Exception as exc:
            return None, f"failed to parse lsjson output: {exc}"

    async def _rclone_check_remote(self, remote_path: str) -> int | None:
        size, err = await self._rclone_stat_remote(remote_path)
        if err:
            logging.warning("Remote stat failed for %s: %s", remote_path, err[:300])
        return size

    async def _rclone_lsjson_dir(self, remote_dir: str) -> tuple[dict[str, int], str]:
        rclone_env = os.environ.copy()
        rclone_env.setdefault("HOME", os.path.expanduser("~"))
        rclone_env["RCLONE_CONFIG"] = Config.RCLONE_CONFIG

        proc = await asyncio.create_subprocess_exec(Config.RCLONE_BIN, "--config", Config.RCLONE_CONFIG, "lsjson", remote_dir, "--files-only", env=rclone_env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        stderr_text = stderr.decode("utf-8", errors="replace").strip()
        if proc.returncode != 0:
            return {}, stderr_text
        if not stdout.strip():
            return {}, ""
        try:
            result: dict[str, int] = {}
            for item in json.loads(stdout.decode()):
                if item.get("IsDir"):
                    continue
                name = item.get("Name") or item.get("Path")
                if name:
                    result[name] = int(item.get("Size", 0))
            return result, ""
        except Exception as exc:
            return {}, f"failed to parse lsjson output: {exc}"

    async def _rclone_lsl_dir(self, remote_dir: str) -> dict[str, int]:
        remote_files, err = await self._rclone_lsjson_dir(remote_dir)
        if err:
            logging.warning("Remote dir listing failed for %s: %s", remote_dir, err[:300])
        return remote_files

    async def _delete_stale_zero_byte_upload_targets(self, success_downloads, local_dir) -> int:
        """
        이번 배치의 업로드 대상 중 원격에 0바이트로 남은 파일만 삭제한다.
        """
        local_map = {
            str(p["path"].relative_to(local_dir)): p
            for p in success_downloads
        }
        dir_groups = {}
        for rel_path in local_map:
            dir_groups.setdefault(os.path.dirname(rel_path), []).append(os.path.basename(rel_path))

        deleted = 0
        for sub_dir, filenames in dir_groups.items():
            remote_dir = f"{Config.RCLONE_REMOTE}/{sub_dir}" if sub_dir else Config.RCLONE_REMOTE
            remote_files = await self._rclone_lsl_dir(remote_dir)
            if not remote_files:
                continue

            for fname in filenames:
                exact_remote_name = self._find_remote_filename(fname, remote_files)
                if exact_remote_name is None: continue
                if remote_files[exact_remote_name] != 0: continue

                remote_full = f"{remote_dir}/{exact_remote_name}"
                ok, err = await self._rclone_delete_remote(remote_full, remote_dir=remote_dir, filename=exact_remote_name)
                if ok:
                    deleted += 1
                    logging.info("Deleted stale 0-byte remote before upload: %s", remote_full)
                elif self._rclone_is_auth_error(err):
                    logging.error("rclone auth failed while deleting stale 0-byte remote: %s", err[:300])
                    return deleted
                else:
                    logging.warning("Failed to delete stale 0-byte remote before upload: %s: %s", remote_full, err[:300])
        return deleted
