import { IsNumber, isNumber, IsObject, IsString } from "class-validator";


export class CreateFileEntryDto {
    @IsString()
    name: string
    @IsString()
    path: string
    @IsNumber()
    size: number
    @IsString()
    peerId: number
}
