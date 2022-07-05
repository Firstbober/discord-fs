const config = {
	guildId: "",
	databaseChannel: "",
	filesChannel: "",
	syncMessage: "",

	botToken: "",
	chunkSize: Math.floor(8388284 / 128) * 128,

	cacheChunkLimit: 16,

	encryptionKey: "",
	encryptionIV: ""
};

import * as crypto from 'crypto';
import * as fs from 'fs'
import * as Path from 'path'

import process from 'node:process';
import Discord from 'discord.js';

import { Readable } from 'stream';
import consola from 'consola'

/**
 * Perform AES-256-CBC on buffers, so discord
 * won't be able to read files
 */
class Encryption {
	ENC_KEY = Buffer.alloc(0);
	IV = Buffer.alloc(0);

	encrypt(buffer: Buffer) {
		this.ENC_KEY = Buffer.from(config.encryptionKey, 'hex');
		this.IV = Buffer.from(config.encryptionIV, 'hex');

		let cipher = crypto.createCipheriv('aes-256-cbc', this.ENC_KEY, this.IV);
		let encrypted = cipher.update(buffer);
		encrypted = Buffer.concat([encrypted, cipher.final()]);
		return encrypted;
	}

	decrypt(buffer: Buffer) {
		this.ENC_KEY = Buffer.from(config.encryptionKey, 'hex');
		this.IV = Buffer.from(config.encryptionIV, 'hex');

		let decipher = crypto.createDecipheriv('aes-256-cbc', this.ENC_KEY, this.IV);
		let decrypted = decipher.update(buffer);
		decrypted = Buffer.concat([decrypted, decipher.final()])
		return decrypted;
	}
}
const encryption = new Encryption()

/**
 * File system index for files, directories and stats
 */
namespace Index {
	export enum INodeType {
		File,
		Directory
	}

	export interface INodeTimestamp {
		creationTime: Date,
		modificationTime: Date,
		accessTime: Date
	}

	export interface INode {
		parent: number,
		inode: number,

		timestamp: INodeTimestamp,
	}

	export interface File extends INode {
		name: string,
		size: number,

		discordPartsIds: string[],
		discordMessageIds: string[]
	}

	export interface Directory extends INode {
		parent: number,
		inode: number

		path: string,
		name: string,

		files: number[]
		directories: number[]
	}

	interface Statistics {
		directories: number,
		files: number,
		totalInodes: number
	}

	export class Manager {
		private _directories: { [key: number]: Directory } = {}
		private _files: { [key: number]: File } = {}
		private _stats: Statistics = {
			directories: 0,
			files: 0,
			totalInodes: 0
		}

		private _freeInodes: number[] = []
		modified = false;

		get directories() {
			return this._directories
		}

		get files() {
			return this._files
		}

		get stats() {
			return this._stats
		}

		init() {
			this.createDirectory("/")
		}

		dirPathTrans(path: string) {
			return path.endsWith("/") ? path : path + "/"
		}

		/**
		 * @param path Path to directory
		 * @returns {number} 0 if no directory, > 0 if directory (inode)
		 */
		hasDirectory(path: string): number {
			for (const dir of Object.values(this._directories)) {
				if (dir.path == path)
					return dir.inode
			}

			return 0
		}
		/**
		 * @param path Path to file
		 * @returns {number} 0 if no file, > 0 if file (inode)
		 */
		hasFile(path: string): number {
			let directory = Path.dirname(path)
			let basename = Path.basename(path)

			let directoryInode = this.hasDirectory(directory)
			if (directoryInode == 0)
				return 0

			for (const childInode of this._directories[directoryInode].files) {
				if (this._files[childInode].name == basename)
					return this._files[childInode].inode
			}

			return 0
		}
		hasPath(path: string): [INodeType, number] | undefined {
			let directory = this.hasDirectory(path)
			let file = this.hasFile(path)

			if (directory == 0 && file == 0)
				return undefined

			return directory > 0 ? [INodeType.Directory, directory] : [INodeType.File, file]
		}

		getFreeInode(): number {
			if (this._freeInodes.length > 0)
				return this._freeInodes.shift()!

			this._stats.totalInodes++
			return this._stats.totalInodes
		}

		getCurrentTimestamp(): INodeTimestamp {
			return {
				creationTime: new Date(),
				modificationTime: new Date(),
				accessTime: new Date(),
			}
		}

		createDirectory(path: string): number {
			if (this.hasPath(path) != undefined) {
				return 0
			}

			let parent = this.hasDirectory(Path.dirname(path))
			let inode = this.getFreeInode()

			this._directories[inode] = {
				parent: parent,
				inode: inode,

				path: path,
				name: Path.basename(path),

				files: [],
				directories: [],

				timestamp: this.getCurrentTimestamp()
			}

			if (parent != 0)
				this._directories[parent].directories.push(inode)
			this._stats.directories += 1;
			this._stats.totalInodes += 1
			this.modified = true;

			return inode
		}
		createFile(directory: string, name: string, size: number, messageIds: string[], parts: string[]): number {
			let parent = this.hasDirectory(directory)

			if (parent == 0)
				return 0

			for (const inode of this._directories[parent].files) {
				let file = this._files[inode]
				if (file.name == name)
					return 0
			}

			let inode = this.getFreeInode()

			this._files[inode] = {
				parent: parent,
				inode: inode,

				size: size,
				name: name,

				discordMessageIds: messageIds,
				discordPartsIds: parts,

				timestamp: this.getCurrentTimestamp()
			}

			if (parent != 0)
				this._directories[parent].files.push(inode)
			this._stats.files += 1;
			this._stats.totalInodes += 1
			this.modified = true;

			return inode
		}

		getDirectory(path: string): Directory | undefined {
			let inode = this.hasDirectory(path);
			if (inode == 0)
				return undefined

			return this._directories[inode]
		}
		getFile(path: string): File | undefined {
			let inode = this.hasFile(path);
			if (inode == 0)
				return undefined

			return this._files[inode]
		}

		updateFileTimestamp(path: string, timestamp: INodeTimestamp): boolean {
			let inode = this.hasFile(path);
			if (inode == 0)
				return false

			this._files[inode].timestamp = timestamp
			this.modified = true

			return true
		}

		unsafeUpdateFile(inode: number, updates: File) {
			for (const key of Object.keys(this._files[inode])) {
				if (this._files[inode][key as keyof File] != updates[key as keyof File]) {
					this._files[inode][key as keyof File] = updates[key as keyof File] as never
					this.modified = true
				}
			}
		}
		unsafeUpdateDir(inode: number, updates: Directory) {
			for (const key of Object.keys(this._files[inode])) {
				if (this._directories[inode][key as keyof Directory] != updates[key as keyof Directory]) {
					this._directories[inode][key as keyof Directory] = updates[key as keyof Directory] as never
					this.modified = true
				}
			}
		}

		removeDir(path: string): boolean {
			let inode = this.hasDirectory(path)
			if (inode == 0)
				return false

			let dir = this._directories[inode]

			let indexInParent = this._directories[dir.parent].directories.indexOf(inode)
			if (indexInParent != -1)
				this._directories[dir.parent].directories.splice(indexInParent, 1)

			for (const directory of dir.directories) {
				delete this._directories[directory]
			}

			for (const file of dir.files) {
				delete this._files[file]
			}

			delete this._directories[inode]

			this._freeInodes.push(inode)

			this._stats.directories -= 1
			this._stats.totalInodes -= 1
			this.modified = true
			return true
		}
		removeFile(path: string) {
			let inode = this.hasFile(path)
			if (inode == 0)
				return false

			let file = this._files[inode]

			let indexInParent = this._directories[file.parent].files.indexOf(inode)
			if (indexInParent != -1)
				this._directories[file.parent].files.splice(indexInParent, 1)

			delete this._files[inode]

			this._freeInodes.push(inode)

			this.modified = true
			this._stats.files -= 1
			this._stats.totalInodes -= 1
			return true
		}

		export() {
			return JSON.stringify({
				directories: this._directories,
				files: this._files,
				stats: this._stats,
				freeInodes: this._freeInodes
			});
		}

		sync(jsonString: string) {
			let json = JSON.parse(jsonString);

			this._directories = json.directories
			this._files = json.files
			this._stats = json.stats
			this._freeInodes = json.freeInodes

			// Timestamp fixer as we get json date and
			// we need JS one
			const fixTimestamp = (dictionary: any) => {
				for (let dir of Object.values(dictionary)) {
					let directory = dir as any

					directory.timestamp.accessTime = new Date(directory.timestamp.accessTime)
					directory.timestamp.creationTime = new Date(directory.timestamp.creationTime)
					directory.timestamp.modificationTime = new Date(directory.timestamp.modificationTime)
				}
			}

			fixTimestamp(this._directories)
			fixTimestamp(this._files)
		}
	}
}

class FSSession {
	bot: Discord.Client | undefined

	dbChannel: Discord.TextChannel | null = null
	filesChannel: Discord.TextChannel | null = null

	index = new Index.Manager()

	indexWatchDog: NodeJS.Timer | undefined
	ratelimitWatchDog: NodeJS.Timer | undefined

	currentRequestAmount: number = 0

	async start() {
		this.bot = new Discord.Client({ intents: [Discord.Intents.FLAGS.GUILD_MESSAGES] });
		this.index.init()

		await this.bot.login(config.botToken);
		let guild = await this.bot.guilds.fetch(config.guildId);

		this.dbChannel = await guild.channels.fetch(config.databaseChannel) as Discord.TextChannel | null
		this.filesChannel = await guild.channels.fetch(config.filesChannel) as Discord.TextChannel | null

		await this.syncIndex(true)
		this.startWatchdog()
	}

	async destroy() {
		await this.syncIndex()
		this.bot!.destroy()

		clearInterval(this.indexWatchDog)
		clearInterval(this.ratelimitWatchDog)
	}

	startWatchdog() {
		this.indexWatchDog = setInterval(async () => {
			await this.syncIndex()
		}, 1000 * 30);

		this.ratelimitWatchDog = setInterval(async () => {
			this.currentRequestAmount = 0
		}, 1000 * 30)
	}

	async syncIndex(initial_sync = false) {
		if (this.index.modified || initial_sync) {
			this.index.modified = false;

			consola.info("[fssession/syncIndex] Starting index synchronization")

			let lastDatabase: {
				fileId: string,
				messageId: string
				sourceId: string
			} = {
				fileId: "",
				messageId: "",
				sourceId: config.syncMessage
			}

			let message = await this.dbChannel?.messages.fetch(lastDatabase.sourceId)
			{
				let data = message!.content.split("\n")

				lastDatabase.fileId = data[2]
				lastDatabase.messageId = data[1]
			}

			if (lastDatabase.messageId != "") {
				if (initial_sync) {
					consola.log("[fssession/syncIndex] Performing initial synchronization")

					let res = await fetch(`https://cdn.discordapp.com/attachments/${config.databaseChannel}/${lastDatabase.fileId}/super-tux-kart.part`)
					this.index.sync(encryption.decrypt(Buffer.from(await res.arrayBuffer())).toString('utf-8'))
					return;
				}
			}

			let indexBuffer = encryption.encrypt(Buffer.from(this.index.export(), 'utf-8'))
			let ids = await this.commitFileToDiscord(indexBuffer, this.dbChannel)

			if (lastDatabase.sourceId == "") {
				await this.dbChannel?.send(`DSCFS-DB#\n${ids[0]}\n${ids[1]}`)
				return
			}

			message = await this.dbChannel?.messages.fetch(lastDatabase.sourceId)
			if (message != undefined)
				await message.edit(`DSCFS-DB#\n${ids[0]}\n${ids[1]}`)

			message = await this.dbChannel?.messages.fetch(lastDatabase.messageId)
			await message?.delete()

			consola.success(`[fssession/syncIndex] Synchronized index with discord`)
		}
	}

	async uploadFile(stream: Readable): Promise<[string[], string[]]> {
		const sleep = (milliseconds: number) => {
			return new Promise(resolve => setTimeout(resolve, milliseconds))
		}

		if (this.currentRequestAmount >= 30) {
			while (this.currentRequestAmount >= 30) {
				await sleep(1000 * 10)
			}

			return this._uploadFile(stream)
		} else {
			return this._uploadFile(stream)
		}
	}

	/**
	 * Real uploadFile
	 * @param stream Readable.
	 * @returns .
	 */
	async _uploadFile(stream: Readable): Promise<[string[], string[]]> {
		let newBuffer = Buffer.alloc(0)
		let currentSize = 0
		let totalSize = 0

		let fileIds: string[] = []
		let messageIds: string[] = []

		for await (let chunk of stream) {
			if (currentSize + Buffer.byteLength(chunk) > config.chunkSize) {
				let toCut = Buffer.byteLength(chunk) - ((currentSize + Buffer.byteLength(chunk)) - config.chunkSize)
				let chunkBuffer = chunk.subarray(0, toCut + 1)
				newBuffer = Buffer.concat([newBuffer, chunkBuffer])

				currentSize = 0

				let idsToPush = await this.commitFileToDiscord(encryption.encrypt(newBuffer))

				messageIds.push(idsToPush[0])
				fileIds.push(idsToPush[1])

				totalSize += Buffer.byteLength(newBuffer)

				newBuffer = Buffer.from([])

				if (toCut == Buffer.byteLength(chunk)) {
					continue;
				}
				chunk = chunk.subarray(toCut + 1)
			}

			currentSize += Buffer.byteLength(chunk)
			newBuffer = Buffer.concat([newBuffer, chunk])
		}

		let idsToPush = await this.commitFileToDiscord(encryption.encrypt(newBuffer))

		messageIds.push(idsToPush[0])
		fileIds.push(idsToPush[1])

		totalSize += Buffer.byteLength(newBuffer)

		return [messageIds, fileIds]
	}

	async commitFileToDiscord(buffer: Buffer, channel = this.filesChannel) {
		let message = await channel!.send({
			files: [
				{
					attachment: buffer,
					name: 'super-tux-kart.part'
				}
			]
		});

		return [
			message.id,
			message.attachments.first()!.id
		]
	}
}

namespace Fuse {
	const Fuse = require("fuse-native")
	const STDIN_INODE = -2
	const STDOUT_INODE = -3
	const STDERR_INODE = -4

	interface DriverOptions {
		mountPath: string
		mkdir: boolean
		libFuseDebug?: boolean,
		displayFolder?: string,
		readOnly: boolean,

		allowOther: boolean,
		allowRoot: boolean,
		autoUnmount: boolean,
		defaultPermissions: boolean,
	}

	interface FDWriteBuffer {
		buffer: Buffer,
		lastPosition: number,
		writtenInto: boolean
	}

	interface FileDescriptor {
		inode: number,
		inodeType?: Index.INodeType,
		writeBuffer: FDWriteBuffer,

		// file descriptor locking, as
		// when copying files werid
		// things were happening.
		inUse: boolean,
		inUseByPath: string
	}

	// I will fill these maybe one day
	// as their only relevant to permissions
	// and file opening modes, and because I do not
	// implement any of there things I am safe.	
	interface INodeMode { }
	interface INodeFlags { }

	interface Operations {
		driver: Driver,

		// (o).(o)

		readdir: (path: string, cb: (...args: any[]) => void) => Promise<void>,
		getattr: (path: string, cb: (...args: any[]) => void) => Promise<void>,
		fgetattr: (path: string, fd: FileDescriptor | undefined, cb: (...args: any[]) => void) => Promise<void>,
		truncate: (path: string, size: number, cb: (...args: any[]) => void) => Promise<void>,
		ftruncate: (path: string, fd: FileDescriptor | undefined, size: number, cb: (...args: any[]) => void) => Promise<void>,
		chmod: (path: string, mode: INodeMode, cb: (...args: any[]) => void) => Promise<void>,
		chown: (path: string, uid: number, gid: number, cb: (...args: any[]) => void) => Promise<void>,
		utimens: (path: string, atime: number, mtime: number, cb: (...args: any[]) => void) => Promise<void>,
		create: (path: string, mode: INodeMode, cb: (...args: any[]) => void) => Promise<void>,
		open: (path: string, flags: INodeFlags, cb: (...args: any[]) => void) => Promise<void>,
		release: (path: string, fd: FileDescriptor | undefined, cb: (...args: any[]) => void) => Promise<void>,
		read: (path: string, fd: FileDescriptor | undefined, buffer: Buffer, length: number, position: number, cb: (...args: any[]) => void) => Promise<void>,
		write: (path: string, fd: FileDescriptor | undefined, buffer: Buffer, length: number, position: number, cb: (...args: any[]) => void) => Promise<void>,
		mkdir: (path: string, mode: INodeMode, cb: (...args: any[]) => void) => Promise<void>,
		rmdir: (path: string, cb: (...args: any[]) => void) => Promise<void>,
		unlink: (path: string, cb: (...args: any[]) => void) => Promise<void>,
		fsync: (path: string, fd: FileDescriptor | undefined, datasync: number, cb: (...args: any[]) => void) => Promise<void>,
	}

	export class Driver {
		fuse: any = undefined
		operations: Operations | undefined = undefined

		fsSession: FSSession = {} as FSSession
		driverOptions: DriverOptions = {
			mountPath: "",
			mkdir: false,
			libFuseDebug: false,
			displayFolder: "dsc-fs",
			readOnly: false,

			allowOther: false,
			allowRoot: false,
			autoUnmount: false,
			defaultPermissions: false,
		}

		/**
		 * Define default file descriptors for
		 * stdin, stdout and stderr
		 */
		fdArray: FileDescriptor[] = [{
			inode: STDIN_INODE,
			writeBuffer: {
				buffer: Buffer.alloc(0),
				lastPosition: 0,
				writtenInto: false,
			},
			inUse: false,
			inUseByPath: ""
		}, {
			inode: STDOUT_INODE,
			writeBuffer: {
				buffer: Buffer.alloc(0),
				lastPosition: 0,
				writtenInto: false
			},
			inUse: false,
			inUseByPath: ""
		}, {
			inode: STDERR_INODE,
			writeBuffer: {
				buffer: Buffer.alloc(0),
				lastPosition: 0,
				writtenInto: false
			},
			inUse: false,
			inUseByPath: ""
		}]

		/**
		 * Cache for already downloaded parts
		 */
		discordPartCache: { [key: string]: Buffer } = {}

		/**
		 * Generates fs stat info based on passed st object
		 */
		generateStatInfo(st: any) {
			let mode = st.mode == Index.INodeType.Directory ? fs.constants.S_IFDIR : (st.mode == Index.INodeType.File ? fs.constants.S_IFREG : st.mode)

			if (this.driverOptions.readOnly) {
				mode |= fs.constants.S_IRUSR | fs.constants.S_IXUSR | fs.constants.S_IRGRP | fs.constants.S_IXGRP | fs.constants.S_IROTH | fs.constants.S_IXOTH
			} else {
				mode |= fs.constants.S_IRWXU | fs.constants.S_IRWXG | fs.constants.S_IRWXO
			}

			return {
				mtime: st.mtime || new Date(),
				atime: st.atime || new Date(),
				ctime: st.ctime || new Date(),
				size: st.size !== undefined ? st.size : 0,
				mode: mode,
				uid: st.uid !== undefined ? st.uid : process.getuid?.(),
				gid: st.gid !== undefined ? st.gid : process.getgid?.(),
				ino: st.ino
			}
		}

		constructor() {
			/**
			 * Heart of the driver, file system operations.
			 */
			this.operations = {
				driver: this,

				async readdir(path, cb) {
					let fsSession = this.driver.fsSession

					let directoryINO = fsSession.index.hasDirectory(path)
					if (directoryINO == 0)
						return cb(-2)


					let result = []
					for (const fileINO of fsSession.index.directories[directoryINO].files) {
						result.push(fsSession.index.files[fileINO].name)
					}

					for (const dirINO of fsSession.index.directories[directoryINO].directories) {
						let dir = fsSession.index.directories[dirINO]
						result.push(dir.name)
					}

					cb(null, result)
				},

				async getattr(path, cb) {
					let fsSession = this.driver.fsSession

					let nodeINO = fsSession.index.hasPath(path)

					if (nodeINO == undefined)
						return cb(-2)

					let node: Index.INode

					if (nodeINO![0] == Index.INodeType.Directory) {
						node = fsSession.index.directories[nodeINO![1]]
					} else {
						node = fsSession.index.files[nodeINO![1]]
					}

					cb(0, this.driver.generateStatInfo({
						ino: nodeINO![1],
						mode: nodeINO![0],
						size: nodeINO![0] == Index.INodeType.Directory ? 0 : (node as Index.File).size,
						atime: node.timestamp.accessTime,
						mtime: node.timestamp.modificationTime,
						ctime: node.timestamp.creationTime
					}))
				},

				async fgetattr(path, fd, cb) {
					return this.getattr(path, cb)
				},

				async truncate(path, size: number, cb) {
					if (this.driver.driverOptions.readOnly)
						return cb(-30)

					let fsSession = this.driver.fsSession
					let fileINO = fsSession.index.hasFile(path)

					if (fileINO == 0)
						return cb(-2)

					let file = fsSession.index.files[fileINO]
					file.size = size
					fsSession.index.unsafeUpdateFile(fileINO, file)

					cb(0)
				},
				async ftruncate(path, fd, size, cb) {
					return this.truncate(path, size, cb)
				},

				async chmod(path, mode, cb) {
					let fsSession = this.driver.fsSession
					if (fsSession.index.hasPath(path) == undefined) return cb(-2)
					cb(0)
				},
				async chown(path, uid, gid, cb) {
					let fsSession = this.driver.fsSession
					if (fsSession.index.hasPath(path) == undefined) return cb(-2)
					cb(0)
				},

				async utimens(path, atime: number, mtime: number, cb) {
					if (this.driver.driverOptions.readOnly)
						return cb(-30)

					let fsSession = this.driver.fsSession

					let pathINO = fsSession.index.hasPath(path)
					if (pathINO == undefined) return cb(-2)

					if (pathINO[0] == Index.INodeType.File) {
						let file = fsSession.index.files[pathINO[1]]

						file.timestamp.accessTime = new Date(atime)
						file.timestamp.modificationTime = new Date(mtime)

						fsSession.index.unsafeUpdateFile(pathINO[1], file)

						return cb(0)
					} else {
						let dir = fsSession.index.directories[pathINO[1]]

						dir.timestamp.accessTime = new Date(atime)
						dir.timestamp.modificationTime = new Date(mtime)

						fsSession.index.unsafeUpdateDir(pathINO[1], dir)

						return cb(0)
					}

					cb(-1)
				},

				async create(path, mode, cb) {
					if (this.driver.driverOptions.readOnly)
						return cb(-30)

					let fsSession = this.driver.fsSession

					if (fsSession.index.hasPath(path) != undefined)
						return cb(-1)

					let newFile = fsSession.index.createFile(Path.dirname(path), Path.basename(path), 0, [], [])
					if (newFile == 0)
						return cb(-5)

					consola.info(`[fuse/driver/ops/create] Created file "${path}" inode(${newFile})`)

					cb(0)
				},

				async open(path, flags, cb) {
					let fsSession = this.driver.fsSession

					let pathINO = fsSession.index.hasPath(path)
					if (pathINO == undefined)
						return cb(-2)

					this.driver.fdArray.push({
						inode: pathINO[1],
						inodeType: pathINO[0],
						writeBuffer: {
							buffer: Buffer.alloc(0),
							lastPosition: 0,
							writtenInto: false
						},
						inUse: true,
						inUseByPath: path
					})

					consola.success(`[fuse/driver/ops/open] Opened a file "${path}" with descriptor ${this.driver.fdArray.length - 1}`)

					cb(0, this.driver.fdArray.length - 1)
				},

				async release(path, fd, cb) {
					let fsSession = this.driver.fsSession

					if (fd != undefined) {
						const fn = async () => {
							let pathINO = fsSession.index.hasPath(path)
							if (pathINO == undefined)
								return

							if (pathINO[0] != Index.INodeType.File)
								return

							if (!fd.writeBuffer.writtenInto)
								return

							let bufferSize = fd.writeBuffer.buffer.length
							let ids = await fsSession.uploadFile(Readable.from(fd.writeBuffer.buffer))

							{
								let inode = pathINO[1]

								let file = fsSession.index.files[inode]
								file.discordMessageIds = ids[0]
								file.discordPartsIds = ids[1]
								file.timestamp.modificationTime = new Date()
								file.size = bufferSize
								fsSession.index.unsafeUpdateFile(inode, file)
							}

							fsSession.syncIndex()

							this.driver.fdArray[this.driver.fdArray.indexOf(fd)].writeBuffer = {
								buffer: Buffer.alloc(0),
								lastPosition: 0,
								writtenInto: false
							}

							this.driver.fdArray[this.driver.fdArray.indexOf(fd)].inUseByPath = ""
							this.driver.fdArray[this.driver.fdArray.indexOf(fd)].inUse = false
						}

						if (!this.driver.driverOptions.readOnly)
							await fn()

						if (fd.inode == STDIN_INODE || fd.inode == STDOUT_INODE || fd.inode == STDERR_INODE)
							return cb(0)

						consola.success(`[fuse/driver/ops/release] Released file descriptor ${fd} with path "${path}"`)

						delete this.driver.fdArray[this.driver.fdArray.indexOf(fd)]

						return cb(0)
					}

					cb(-1)
				},

				async read(path, fd, buffer: Buffer, length: number, position: number, cb) {
					let fsSession = this.driver.fsSession

					if (fd == undefined)
						return cb(-1)

					let pathINO = fsSession.index.hasPath(path)
					if (pathINO == undefined)
						return cb(-2)

					if (pathINO[0] == Index.INodeType.Directory)
						return cb(-1)

					let fileInfo = fsSession.index.files[pathINO[1]]

					if (position >= fileInfo.size) return cb(0)

					if (!this.driver.driverOptions.readOnly) {
						let file = fsSession.index.files[pathINO[1]]
						file.timestamp.accessTime = new Date()
						fsSession.index.unsafeUpdateFile(pathINO[1], file)
					}

					let partStart = Math.ceil(position / config.chunkSize)
					let partEnd = Math.ceil((position + length) / config.chunkSize)

					partStart = partStart < 1 ? 1 : partStart

					let extractableBuffer = Buffer.alloc(0)

					for (let partIdx = partStart - 1; partIdx < partEnd; partIdx++) {
						let partID = fileInfo.discordPartsIds[partIdx];

						if (this.driver.discordPartCache[partID] == undefined) {
							let res = await fetch(`https://cdn.discordapp.com/attachments/${config.filesChannel}/${partID}/super-tux-kart.part`)
							let bufferRes = await res.arrayBuffer()

							this.driver.discordPartCache[partID] = encryption.decrypt(Buffer.from(bufferRes))

							consola.info(`[fuse/driver/ops/read] Downloaded and cached part ${partID} for reading`)
							this.driver.collectCache()
						}

						extractableBuffer = Buffer.concat([extractableBuffer, this.driver.discordPartCache[partID]])
					}

					let positionInPart = position - ((partStart - 1) * (config.chunkSize + 1))
					let final = extractableBuffer.subarray(positionInPart, positionInPart + length)
					final.copy(buffer)

					cb(Buffer.byteLength(final))

					consola.success(`[fuse/driver/ops/read] Finished reading of fd(${fd}) with pos(${position}), len(${length}), relPos(${positionInPart})`)
				},

				async write(path, fd, buffer: Buffer, length: number, position: number, cb) {
					if (this.driver.driverOptions.readOnly)
						return cb(-30)

					let fsSession = this.driver.fsSession

					if (fd == undefined)
						return cb(-1)

					let pathINO = fsSession.index.hasPath(path)
					if (pathINO == undefined)
						return cb(-2)

					if (pathINO[0] == Index.INodeType.Directory)
						return cb(-1)

					const sleep = (milliseconds: number) => {
						return new Promise(resolve => setTimeout(resolve, milliseconds))
					}

					while (fd.inUse && fd.inUseByPath != path) {
						consola.warn(`[fuse/driver/ops/write] File descriptor fd(${this.driver.fdArray.indexOf(fd)}) is in use. Waiting 500 ms...`)
						await sleep(500)
					}

					fd.inUse = true
					fd.inUseByPath = path

					if (fd.writeBuffer.buffer.length < position + length) {
						fd.writeBuffer.buffer = Buffer.concat([
							fd.writeBuffer.buffer,
							Buffer.alloc((position + length) - fd.writeBuffer.buffer.length)
						])
					}

					if (fd.writeBuffer.lastPosition > position) {
						consola.warn(`[fuse/driver/ops/write] Last writeStream position was ${fd.writeBuffer.lastPosition} now it is ${position} - implement writing into already pushed data`)
						buffer.copy(fd.writeBuffer.buffer, fd.writeBuffer.lastPosition)
					} else {
						buffer.copy(fd.writeBuffer.buffer, position)
					}

					fd.writeBuffer.lastPosition = position
					fd.writeBuffer.writtenInto = true

					cb(length)
				},

				async mkdir(path, mode, cb) {
					if (this.driver.driverOptions.readOnly)
						return cb(-30)

					let fsSession = this.driver.fsSession

					let pathINO = fsSession.index.hasPath(path)
					if (pathINO != undefined)
						return cb(-1)

					fsSession.index.createDirectory(path)
					return cb(0)
				},
				async rmdir(path, cb) {
					if (this.driver.driverOptions.readOnly)
						return cb(-30)

					let fsSession = this.driver.fsSession
					if (!fsSession.index.removeDir(path))
						return cb(-1)

					return cb(0)
				},

				async unlink(path, cb) {
					if (this.driver.driverOptions.readOnly)
						return cb(-30)

					let fsSession = this.driver.fsSession

					let pathINO = fsSession.index.hasPath(path)
					if (pathINO == undefined)
						return cb(-1)

					if (pathINO![0] != Index.INodeType.File)
						return cb(-21)

					let file = fsSession.index.files[pathINO[1]]
					for (const discordId of file.discordMessageIds) {
						let message = await fsSession.filesChannel?.messages.fetch(discordId)
						await message?.delete()
					}

					if (!fsSession.index.removeFile(path))
						return cb(-1)

					cb(0)
				},

				async fsync(path, fd, datasync, cb) {
					if (this.driver.driverOptions.readOnly)
						return cb(-30)

					let fsSession = this.driver.fsSession

					if (fd == undefined)
						return cb(-1)

					if (fd.inodeType != Index.INodeType.File)
						return cb(0)

					if (!fd.writeBuffer.writtenInto)
						return cb(0)

					consola.info(`[fuse/driver/ops/fsync] Called fsync while writing into the file - pushing into discord`)

					let ids = await fsSession.uploadFile(Readable.from(fd.writeBuffer.buffer))

					{
						let inode = fsSession.index.hasPath(path)![1]

						let file = fsSession.index.files[inode]
						file.discordMessageIds = ids[0]
						file.discordPartsIds = ids[1]
						file.timestamp.modificationTime = new Date()
						file.size = fd.writeBuffer.buffer.length
						fsSession.index.unsafeUpdateFile(inode, file)
					}

					await fsSession.syncIndex()

					fd.writeBuffer = {
						buffer: Buffer.alloc(0),
						lastPosition: 0,
						writtenInto: false
					}

					cb(0)
				}
			}
		}

		/**
		 * Start and mount fuse filesystem
		 * @param driverOptions Options for driver
		 */
		async start(driverOptions: DriverOptions) {
			this.fsSession = new FSSession()
			await this.fsSession.start()

			this.driverOptions = driverOptions

			// Create Fuse instance with mount path and
			// wrapped operations for async
			this.fuse = new Fuse(this.driverOptions.mountPath, {
				readdir: (path: string, cb: any) => {
					this.operations?.readdir(path, cb)
				},

				getattr: (path: string, cb: any) => {
					this.operations?.getattr(path, cb)
				},

				fgetattr: (path: string, fd: number, cb: any) => {
					this.operations?.fgetattr(path, this.fdArray[fd], cb)
				},

				truncate: (path: string, size: number, cb: any) => {
					this.operations?.truncate(path, size, cb)
				},
				ftruncate: (path: string, fd: number, size: number, cb: any) => {
					this.operations?.ftruncate(path, this.fdArray[fd], size, cb)
				},

				chmod: (path: string, mode: number, cb: any) => {
					this.operations?.chmod(path, {}, cb)
				},
				chown: (path: string, uid: number, gid: number, cb: any) => {
					this.operations?.chown(path, uid, gid, cb)
				},

				utimens: (path: string, atime: number, mtime: number, cb: any) => {
					this.operations?.utimens(path, atime, mtime, cb)
				},

				create: (path: string, mode: any, cb: any) => {
					this.operations?.create(path, {}, cb)
				},

				open: (path: string, flags: any, cb: any) => {
					this.operations?.open(path, {}, cb)
				},

				release: (path: string, fd: number, cb: any) => {
					this.operations?.release(path, this.fdArray[fd], cb)
				},

				read: (path: string, fd: number, buffer: Buffer, length: number, position: number, cb: any) => {
					this.operations?.read(path, this.fdArray[fd], buffer, length, position, cb)
				},

				write: (path: string, fd: number, buffer: Buffer, length: number, position: number, cb: any) => {
					this.operations?.write(path, this.fdArray[fd], buffer, length, position, cb)
				},

				mkdir: (path: string, mode: any, cb: any) => {
					this.operations?.mkdir(path, {}, cb)
				},
				rmdir: (path: string, cb: any) => {
					this.operations?.rmdir(path, cb)
				},

				unlink: (path: string, cb: any) => {
					this.operations?.unlink(path, cb)
				},

				fsync: (path: string, fd: number, datasync: number, cb: any) => {
					this.operations?.fsync(path, this.fdArray[fd], datasync, cb)
				}
			}, {
				debug: this.driverOptions.libFuseDebug,
				displayFolder: this.driverOptions.displayFolder,
				mkdir: this.driverOptions.mkdir,
				fsname: this.driverOptions.displayFolder,

				allowOther: this.driverOptions.allowOther,
				allowRoot: this.driverOptions.allowRoot,
				autoUnmount: this.driverOptions.autoUnmount,
				defaultPermissions: this.driverOptions.defaultPermissions,
			})

			// Overwrite build-in fuse-native function
			// so we can tinker with custom fuse options
			this.fuse._fuseOptions = function () {
				const options = []

				if ((/\*|(^,)fuse-bindings(,$)/.test(process.env.DEBUG!)) || this.opts.debug) options.push('debug')
				if (this.opts.allowOther) options.push('allow_other')
				if (this.opts.allowRoot) options.push('allow_root')
				if (this.opts.autoUnmount) options.push('auto_unmount')
				if (this.opts.defaultPermissions) options.push('default_permissions')
				if (this.opts.blkdev) options.push('blkdev')
				if (this.opts.blksize) options.push('blksize=' + this.opts.blksize)
				if (this.opts.maxRead) options.push('max_read=' + this.opts.maxRead)
				if (this.opts.fd) options.push('fd=' + this.opts.fd)
				if (this.opts.userId) options.push('user_id=', this.opts.userId)
				if (this.opts.fsname) options.push('fsname=' + this.opts.fsname)
				if (this.opts.subtype) options.push('subtype=' + this.opts.subtype)
				if (this.opts.kernelCache) options.push('kernel_cache')
				if (this.opts.autoCache) options.push('auto_cache')
				if (this.opts.umask) options.push('umask=' + this.opts.umask)
				if (this.opts.uid) options.push('uid=' + this.opts.uid)
				if (this.opts.gid) options.push('gid=' + this.opts.gid)
				if (this.opts.entryTimeout) options.push('entry_timeout=' + this.opts.entryTimeout)
				if (this.opts.attrTimeout) options.push('attr_timeout=' + this.opts.attrTimeout)
				if (this.opts.acAttrTimeout) options.push('ac_attr_timeout=' + this.opts.acAttrTimeout)
				if (this.opts.noforget) options.push('noforget')
				if (this.opts.remember) options.push('remember=' + this.opts.remember)
				if (this.opts.modules) options.push('modules=' + this.opts.modules)

				return options.length ? '-o' + options.join(',') : ''
			}

			this.fuse!.mount((mountError: any) => {
				if (mountError) {
					consola.error(mountError)

					this.fsSession!.destroy().then(() => {
						process.exit()
					})
					return
				}

				consola.success(`[fuse/driver/start] Mounted fuse driver!`)

				if (this.driverOptions.readOnly) {
					consola.log(`[fuse/driver/start] This is a read only filesystem`)
				}
			})
		}
		async stop() {
			Fuse.unmount(this.driverOptions.mountPath, () => { })
			await this.fsSession!.destroy()
		}

		async collectCache() {
			if (Object.keys(this.discordPartCache).length > config.cacheChunkLimit) {
				let overflow = Object.keys(this.discordPartCache).length - config.cacheChunkLimit;

				for (const key of Object.keys(this.discordPartCache).slice(0, overflow + 1)) {
					delete this.discordPartCache[key]
					consola.info(`[fuse/driver/collectCache] Removed cache of part ${key}`)
				}
			}
		}
	}
}

import { program } from 'commander'
let fuseDriver: Fuse.Driver

program
	.name('dscfs')
	.description('Discord File System CLI and Fuse driver')
	.version('1.0.0')

program.command('init')
	.description('Creates config and performs initial synchronization with discord')
	.argument('<path>', 'where to place the config')
	.argument('<botToken>', 'discord bot token to use')
	.argument('<guildId>', 'discord guild id')
	.argument('<databaseChannelId>', 'discord id for database channel')
	.argument('<filesChannelId>', 'discord id for files channel')
	.action(async (path: string, botToken: string, guildId: string, databaseChannelId: string, filesChannelId: string) => {
		let bot = new Discord.Client({ intents: [Discord.Intents.FLAGS.GUILD_MESSAGES] })

		bot.on('ready', async () => {
			consola.info('Checking validity of passed parameters...')

			let guild = await bot.guilds.fetch(guildId);

			let dbChannel = await guild.channels.fetch(databaseChannelId) as Discord.TextChannel | null
			let filesChannel = await guild.channels.fetch(filesChannelId) as Discord.TextChannel | null

			if (dbChannel == null || filesChannel == null) {
				bot.destroy()
				consola.error('Database and files channel ids are invalid')
				process.exit()
			}

			let index = new Index.Manager()
			index.init()

			let encryptionKey = crypto.randomBytes(32).toString('hex')
			let encryptionIV = crypto.randomBytes(16).toString('hex')

			config.encryptionKey = encryptionKey
			config.encryptionIV = encryptionIV

			let message = await dbChannel.send({
				files: [
					{
						attachment: encryption.encrypt(Buffer.from(index.export(), 'utf-8')),
						name: 'super-tux-kart.part'
					}
				]
			});

			consola.info('Parameters determined to be valid - send data to discord and writing config...')

			let syncMessage = await dbChannel.send(`DSCFS-DB#\n${message.id}\n${message.attachments.first()!.id}`)

			fs.writeFileSync(path, JSON.stringify({
				guildId: guildId,
				databaseChannel: databaseChannelId,
				filesChannel: filesChannelId,
				syncMessage: syncMessage.id,

				botToken: botToken,
				chunkSize: Math.floor(8388284 / 128) * 128,

				cacheChunkLimit: 16,

				encryptionKey,
				encryptionIV
			}, undefined, 4))

			consola.success('Intialization was successful, exiting')

			bot.destroy()
			process.exit()
		})

		bot.on('error', () => {
			consola.error('Failed to use provided bot token')
			process.exit()
		})

		await bot.login(botToken)
	})

program.command('mount')
	.description('Mount filesystem to specified path')
	.argument('<configPath>', 'path to config')
	.argument('<path>', 'path to use')

	.option('--readOnly', 'mount as read only')
	.option('--mkdir', 'create mountpoint before mounting')
	.option('--allowOther', 'overrides the security measure restricting file access to the filesystem owner, so that all users (including root) can access the files.')
	.option('--allowRoot', 'This option is similar to allowOther but file access is limited to the filesystem owner and root.')
	.option('--autoUnmount', 'This option enables automatic release of the mountpoint if filesystem terminates for any reason.')
	.option('--defaultPermissions', 'This option instructs the kernel to perform its own permission check instead of deferring all permission checking to the filesystem.')

	.action(async (configPath: string, path: string, options: any) => {
		let readConfig = JSON.parse(fs.readFileSync(configPath).toString())

		for (const key of Object.keys(config)) {
			if (readConfig[key] == undefined) {
				consola.error(`Invalid config, lacks "${key}" property`)
				process.exit()
			} else {
				(config as any)[key] = readConfig[key]
			}
		}

		consola.success('Loaded config, attempting to mount filesystem')

		fuseDriver = new Fuse.Driver()

		await fuseDriver.start({
			mountPath: path,
			mkdir: options.mkdir != undefined,
			readOnly: options.readOnly != undefined,

			allowOther: options.allowOther != undefined,
			allowRoot: options.allowRoot != undefined,
			autoUnmount: options.autoUnmount != undefined,
			defaultPermissions: options.defaultPermissions != undefined,
		})
	})

process.on('SIGINT', async () => {
	await fuseDriver.stop()
	process.exit()
})

process.on('exit', async () => {
	await fuseDriver.stop()
	process.exit()
})

program.parse()