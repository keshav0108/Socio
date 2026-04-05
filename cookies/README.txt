Instagram cookies for yt-dlp (Hostinger / FastAPI)
==================================================

Your n8n workflow only calls your API. yt-dlp runs ON THE SERVER, so the
cookies file must exist on Hostinger, not inside n8n.

1. In Chrome: install "Get cookies.txt LOCALLY" (or similar). Log in to Instagram.
2. Export cookies (Netscape format). Do NOT use cookies.example.txt as-is — it is only a format sample.
3. Save your real export as `instagram_cookies.txt` next to this README (same folder), or upload that file to your server:
      /path/to/Socio/cookies/instagram_cookies.txt
4. On Hostinger, edit the deployment .env (same folder as api.py) and add ONE line with the
   REAL path (not a placeholder like /full/path/on/server/to/cookies.txt — that will never work):

      YTDLP_COOKIE_FILE=/home/USER/domains/YOURDOMAIN/public_html/Socio/cookies/instagram_cookies.txt

   Use File Manager or SSH `pwd` / `realpath` to get the exact path. The Linux user running Python
   must be able to read this file (chmod 644 on cookies.txt, parent folders executable).

5. Restart the Python app (uvicorn / systemd / hosting panel).

6. `instagram_cookies.txt` and `cookies.txt` are gitignored. `cookies.example.txt` is safe to commit (no secrets).

Optional alias:

      INSTAGRAM_COOKIES_FILE=/same/path/as/above

If the path is wrong or the file is missing, the API will return a clear error.
