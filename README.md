# discord-fs
FUSE File System implemented in TS and Discord Attachments

`npm i` then `npm start`

```
Usage: dscfs [options] [command]

Discord File System CLI and Fuse driver

Options:
  -V, --version                                                          output the version number
  -h, --help                                                             display help for command

Commands:
  init <path> <botToken> <guildId> <databaseChannelId> <filesChannelId>  Creates config and performs initial synchronization with discord
  mount [options] <configPath> <path>                                    Mount filesystem to specified path
  help [command]                                                         display help for command
```
