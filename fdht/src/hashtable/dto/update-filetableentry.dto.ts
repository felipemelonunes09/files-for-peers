import { PartialType } from '@nestjs/mapped-types';
import { IsNumber, isNumber, IsObject, IsString } from "class-validator";
import { CreateFileEntryDto } from './create-filetableentry.dto';

export class UpdateFileEntryDto{
    @IsString()
    name: string

    @IsString()
    path: string

    @IsNumber()
    size: number
}
