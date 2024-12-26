import { Controller, Get, Post, Body, Patch, Param, Delete, UseInterceptors, UploadedFile } from '@nestjs/common';
import { ResourcesService } from './resources.service';
import { FileInterceptor } from '@nestjs/platform-express';
import { Resource } from './entities/resource.entity';
import { HashtableService } from 'src/hashtable/hashtable.service';

// ### Dev notes ### //
// -> this is using resolution 001 and 002
//

@Controller('resources')
export class ResourcesController {
  constructor(private readonly resourcesService: ResourcesService, private readonly hashtableService: HashtableService) {}
  
  @Post('upload')
  @UseInterceptors(FileInterceptor('file'))
  uploadFile(@UploadedFile() file: Express.Multer.File) {
    return this.hashtableService.create({ name: file.originalname, path: file.path, size: file.size})
  }

  @Get()
  findAll(): Array<Resource> {
    return this.resourcesService.getFilesLocation();
  }
}
