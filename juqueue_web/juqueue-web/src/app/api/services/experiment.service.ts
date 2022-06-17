/* tslint:disable */
/* eslint-disable */
import { Injectable } from '@angular/core';
import { HttpClient, HttpResponse } from '@angular/common/http';
import { BaseService } from '../base-service';
import { ApiConfiguration } from '../api-configuration';
import { StrictHttpResponse } from '../strict-http-response';
import { RequestBuilder } from '../request-builder';
import { Observable } from 'rxjs';
import { map, filter } from 'rxjs/operators';

import { BodyCancelRunsExperimentExperimentNameCancelRunsPut } from '../models/body-cancel-runs-experiment-experiment-name-cancel-runs-put';
import { Experiment } from '../models/experiment';
import { SuccessResponse } from '../models/success-response';

@Injectable({
  providedIn: 'root',
})
export class ExperimentService extends BaseService {
  constructor(
    config: ApiConfiguration,
    http: HttpClient
  ) {
    super(config, http);
  }

  /**
   * Path part for operation getExperiments
   */
  static readonly GetExperimentsPath = '/experiments';

  /**
   * Get Experiments.
   *
   * Returns all experiments.
   *
   * This method provides access to the full `HttpResponse`, allowing access to response headers.
   * To access only the response body, use `getExperiments()` instead.
   *
   * This method doesn't expect any request body.
   */
  getExperiments$Response(params?: {
  }): Observable<StrictHttpResponse<{
[key: string]: Experiment;
}>> {

    const rb = new RequestBuilder(this.rootUrl, ExperimentService.GetExperimentsPath, 'get');
    if (params) {
    }

    return this.http.request(rb.build({
      responseType: 'json',
      accept: 'application/json'
    })).pipe(
      filter((r: any) => r instanceof HttpResponse),
      map((r: HttpResponse<any>) => {
        return r as StrictHttpResponse<{
        [key: string]: Experiment;
        }>;
      })
    );
  }

  /**
   * Get Experiments.
   *
   * Returns all experiments.
   *
   * This method provides access to only to the response body.
   * To access the full response (for headers, for example), `getExperiments$Response()` instead.
   *
   * This method doesn't expect any request body.
   */
  getExperiments(params?: {
  }): Observable<{
[key: string]: Experiment;
}> {

    return this.getExperiments$Response(params).pipe(
      map((r: StrictHttpResponse<{
[key: string]: Experiment;
}>) => r.body as {
[key: string]: Experiment;
})
    );
  }

  /**
   * Path part for operation resumeRuns
   */
  static readonly ResumeRunsPath = '/experiment/{experiment_name}/resume_runs';

  /**
   * Resume Runs.
   *
   * Resumes runs of given experiment if not running already
   *
   * - **run_ids**: List of run identifiers
   *
   * This method provides access to the full `HttpResponse`, allowing access to response headers.
   * To access only the response body, use `resumeRuns()` instead.
   *
   * This method sends `application/json` and handles request body of type `application/json`.
   */
  resumeRuns$Response(params: {
    experiment_name: string;
    body: Array<string>
  }): Observable<StrictHttpResponse<SuccessResponse>> {

    const rb = new RequestBuilder(this.rootUrl, ExperimentService.ResumeRunsPath, 'put');
    if (params) {
      rb.path('experiment_name', params.experiment_name, {});
      rb.body(params.body, 'application/json');
    }

    return this.http.request(rb.build({
      responseType: 'json',
      accept: 'application/json'
    })).pipe(
      filter((r: any) => r instanceof HttpResponse),
      map((r: HttpResponse<any>) => {
        return r as StrictHttpResponse<SuccessResponse>;
      })
    );
  }

  /**
   * Resume Runs.
   *
   * Resumes runs of given experiment if not running already
   *
   * - **run_ids**: List of run identifiers
   *
   * This method provides access to only to the response body.
   * To access the full response (for headers, for example), `resumeRuns$Response()` instead.
   *
   * This method sends `application/json` and handles request body of type `application/json`.
   */
  resumeRuns(params: {
    experiment_name: string;
    body: Array<string>
  }): Observable<SuccessResponse> {

    return this.resumeRuns$Response(params).pipe(
      map((r: StrictHttpResponse<SuccessResponse>) => r.body as SuccessResponse)
    );
  }

  /**
   * Path part for operation cancelRuns
   */
  static readonly CancelRunsPath = '/experiment/{experiment_name}/cancel_runs';

  /**
   * Cancel Runs.
   *
   * Cancels runs of given experiment.
   *
   * - **run_ids**: List of run identifiers
   * - **force**: Forces interruption of runs that are already running
   *
   * This method provides access to the full `HttpResponse`, allowing access to response headers.
   * To access only the response body, use `cancelRuns()` instead.
   *
   * This method sends `application/json` and handles request body of type `application/json`.
   */
  cancelRuns$Response(params: {
    experiment_name: string;
    body: BodyCancelRunsExperimentExperimentNameCancelRunsPut
  }): Observable<StrictHttpResponse<SuccessResponse>> {

    const rb = new RequestBuilder(this.rootUrl, ExperimentService.CancelRunsPath, 'put');
    if (params) {
      rb.path('experiment_name', params.experiment_name, {});
      rb.body(params.body, 'application/json');
    }

    return this.http.request(rb.build({
      responseType: 'json',
      accept: 'application/json'
    })).pipe(
      filter((r: any) => r instanceof HttpResponse),
      map((r: HttpResponse<any>) => {
        return r as StrictHttpResponse<SuccessResponse>;
      })
    );
  }

  /**
   * Cancel Runs.
   *
   * Cancels runs of given experiment.
   *
   * - **run_ids**: List of run identifiers
   * - **force**: Forces interruption of runs that are already running
   *
   * This method provides access to only to the response body.
   * To access the full response (for headers, for example), `cancelRuns$Response()` instead.
   *
   * This method sends `application/json` and handles request body of type `application/json`.
   */
  cancelRuns(params: {
    experiment_name: string;
    body: BodyCancelRunsExperimentExperimentNameCancelRunsPut
  }): Observable<SuccessResponse> {

    return this.cancelRuns$Response(params).pipe(
      map((r: StrictHttpResponse<SuccessResponse>) => r.body as SuccessResponse)
    );
  }

}
