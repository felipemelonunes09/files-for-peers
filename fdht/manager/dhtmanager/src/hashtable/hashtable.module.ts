import { Module } from '@nestjs/common';
import { HashtableService } from './hashtable.service';
import { HashtableController } from './hashtable.controller';

@Module({
  exports: [HashtableService],
  controllers: [HashtableController],
  providers: [HashtableService],
})
export class HashtableModule {}
