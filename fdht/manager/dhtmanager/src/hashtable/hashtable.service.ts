import { BadRequestException, HttpCode, HttpException, HttpStatus, Injectable, InternalServerErrorException, NotFoundException } from '@nestjs/common';
import { Hashtable } from './entities/hashtable.entity';
import { ConfigService } from '@nestjs/config'
import { promises as fs } from 'fs'; 
import { CreateFileEntryDto } from './dto/create-filetableentry.dto';
import { FileTableEntry } from './entities/filetableentry.entity';
import { UpdateFileEntryDto } from './dto/update-filetableentry.dto';

@Injectable()
export class HashtableService {
  private table: Hashtable
  
  constructor(private configService: ConfigService) {
    this.loadHashTable()
  }

  private async loadHashTable(): Promise<void> {
    try {
      const filePath = this.configService.get("hashtableFilePath")
      this.table = new Hashtable()
      const data = await fs.readFile(filePath, 'utf-8')
      this.table = JSON.parse(data)
    }
    catch(err) {
      if (err.code === 'ENOENT') {
        console.log("Error when finding the hashtable: --resolution: creating file")
        await this.createHashTable()
      }
    }
  }

  private async createHashTable(): Promise<void>  {
    await fs.mkdir(this.configService.get("hashtableFileDir"), { recursive: true })
    this.saveHashTable()
  }

  private async saveHashTable():Promise<void>  {
     fs.writeFile(this.configService.get("hashtableFilePath"), JSON.stringify(this.table))
  }

  async create(createFileEntryDto: CreateFileEntryDto): Promise<FileTableEntry> {
    const fileEntry = Object.assign(new FileTableEntry(), createFileEntryDto)
    const currentDate = new Date()
    fileEntry.createdAt = currentDate
    fileEntry.updatedAt = currentDate

    if (this.table[fileEntry.name] == undefined) {
      this.table[fileEntry.name] = fileEntry
      this.saveHashTable()
      return fileEntry
    }
    else
      throw new HttpException("File name it's already in the hashtable", HttpStatus.BAD_REQUEST);
  }

  async findAll(): Promise<Hashtable> {
    return this.table
  }

  async findOne(key: string): Promise<FileTableEntry> {
    if (this.table[key] == undefined)
      throw new HttpException("File not found", HttpStatus.NOT_FOUND)
    return this.table[key]
  }

  async update(key: string, updateFileEntryDto: UpdateFileEntryDto): Promise<FileTableEntry> {
    if (this.table[updateFileEntryDto.name] == undefined)
      throw new HttpException("File not found", HttpStatus.NOT_FOUND)

    const fileEntry = Object.assign(new FileTableEntry(), updateFileEntryDto)
    const currentDate = new Date()
    fileEntry.updatedAt = currentDate
    this.table[updateFileEntryDto.name] = fileEntry
    return fileEntry
  }

  async remove(key: string):Promise<void> {
    if (this.table[key] == undefined)
      throw new HttpException("File not found", HttpStatus.NOT_FOUND)
    delete this.table[key]
    this.saveHashTable()
  }
}
