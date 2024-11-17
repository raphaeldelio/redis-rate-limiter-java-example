Fixed Window Rate Limiter with Redis and Java

This repository contains the implementation of a Fixed Window Rate Limiter using Redis and Java, as described in the accompanying article. The limiter enforces request limits within fixed time intervals, leveraging Redis commands like INCR and EXPIRE for fast and reliable rate control.

Features

	•	Tracks requests per client using Redis keys.
	•	Supports automatic counter reset after the time window expires.
	•	Independent handling of multiple clients.

For more details, read the full article. (Yet to be published)

Contributions and feedback are welcome! 🎉
