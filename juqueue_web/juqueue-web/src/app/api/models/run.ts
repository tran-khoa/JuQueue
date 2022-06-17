/* tslint:disable */
/* eslint-disable */
import { RunDef } from './run-def';

/**
 * Current instance of a run definition
 */
export interface Run {

  /**
   * Date and time of creation.
   */
  created_at: string;
  run_def: RunDef;

  /**
   * Current status of the run.
   */
  status: 'running' | 'ready' | 'failed' | 'inactive' | 'finished';
}
