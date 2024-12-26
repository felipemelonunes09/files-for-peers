import { HttpException, HttpStatus, Injectable } from '@nestjs/common';
import { Resource } from './entities/resource.entity';
import { readdirSync, statSync } from 'fs';
import { extname, join } from 'path';
import { ConfigService } from '@nestjs/config'

@Injectable()
export class ResourcesService {
  // should be in a configuration service
  private readonly uploadDir = './resources'

  constructor(private configService: ConfigService) {}

  public getFilesLocation(): Array<Resource> {
    try {
      const uploadDir = this.configService.get<string>('resourcesDirPath')
      const files = readdirSync(this.uploadDir);
      const resources: Resource[] = files.map(file => {
        const filePath = join(this.uploadDir, file)
        const fileStats = statSync(filePath)
        return {
          name: file,
          path: filePath,
          size: fileStats.size,
          extesion: extname(file)
        }
      })
      return resources
    }
    catch (err) {
      if (err.code === "ENOENT") 
        throw new HttpException('File or directory not found', HttpStatus.NOT_FOUND)
      if (err.code == "EACCES")
        throw new HttpException('No permission to access the file or directory', HttpStatus.FORBIDDEN)

      throw new HttpException('An unexpected error corred while acessing dir', HttpStatus.INTERNAL_SERVER_ERROR)
    }
  }
}
