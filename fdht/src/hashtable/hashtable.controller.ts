import { Controller, Get, Post, Body, Patch, Param, Delete } from '@nestjs/common';
import { HashtableService } from './hashtable.service';
import { CreateFileEntryDto } from './dto/create-filetableentry.dto';
import { UpdateFileEntryDto } from './dto/update-filetableentry.dto';

@Controller('hashtable')
export class HashtableController {
  constructor(private readonly hashtableService: HashtableService) {}

  @Post()
  async create(@Body() createFileEntryDto: CreateFileEntryDto) {
    return this.hashtableService.create(createFileEntryDto);
  }

  @Get()
  async findAll() {
    return this.hashtableService.findAll();
  }

  @Get(':name')
  async findOne(@Param('name') key: string) {
    return this.hashtableService.findOne(key);
  }

  @Patch(':name')
  async update(@Param('name') key: string, @Body() updateFileEntryDto: UpdateFileEntryDto) {
    return this.hashtableService.update(key, updateFileEntryDto);
  }

  @Delete(':name')
  async remove(@Param('name') key: string) {
    return this.hashtableService.remove(key);
  }
}
