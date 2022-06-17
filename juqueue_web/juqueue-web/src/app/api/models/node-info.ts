/* tslint:disable */
/* eslint-disable */
import { SlotInfo } from './slot-info';
export interface NodeInfo {
  slots: Array<SlotInfo>;
  status: 'queued' | 'dead' | 'alive';
  worker: string;
}
