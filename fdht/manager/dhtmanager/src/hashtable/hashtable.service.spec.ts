import { Test, TestingModule } from '@nestjs/testing';
import { HashtableService } from './hashtable.service';

describe('HashtableService', () => {
  let service: HashtableService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [HashtableService],
    }).compile();

    service = module.get<HashtableService>(HashtableService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
