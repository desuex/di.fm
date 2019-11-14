# Async di.fm parser

Create your own music playlists

First run init command to create SQLite tables:

```sh
python3 main.py -i -d
```

To parse all the precious di.fm data, use next command:

```sh
python3 main.py -c -t -a -w -d
```

If you want to have YouTube links of your songs, use:

```sh
python3 main.py -l -d
```

Note: `-d` flag is optional, but it helps you to understand what's actually happening

Anyway, enjoy your music! 
