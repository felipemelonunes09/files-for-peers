import { Module } from '@nestjs/common';
import { ResourcesService } from './resources.service';
import { ResourcesController } from './resources.controller';
import { MulterModule } from '@nestjs/platform-express';
import { ConfigModule } from '@nestjs/config'
import { diskStorage } from 'multer';
import { extname } from 'path';
import { HashtableModule } from 'src/hashtable/hashtable.module';

@Module({
  imports: [
    MulterModule.register({
      storage: diskStorage({
        destination: './resources',
        filename: (req, file, callback) => {
          const extension = extname(file.originalname)
          callback(null, `${file.originalname}${extension}`)
        }
      })
    }),
    ConfigModule,
    HashtableModule
  ],
  controllers: [ResourcesController],
  providers: [ResourcesService],
})
export class ResourcesModule {}
