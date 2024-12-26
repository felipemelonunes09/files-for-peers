import { Test, TestingModule } from '@nestjs/testing';
import { HashtableController } from './hashtable.controller';
import { HashtableService } from './hashtable.service';

describe('HashtableController', () => {
  let controller: HashtableController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [HashtableController],
      providers: [HashtableService],
    }).compile();

    controller = module.get<HashtableController>(HashtableController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
