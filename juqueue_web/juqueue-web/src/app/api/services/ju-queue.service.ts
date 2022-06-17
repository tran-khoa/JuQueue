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

import { SuccessResponse } from '../models/success-response';

@Injectable({
  providedIn: 'root',
})
export class JuQueueService extends BaseService {
  constructor(
    config: ApiConfiguration,
    http: HttpClient
  ) {
    super(config, http);
  }

  /**
   * Path part for operation stop
   */
  static readonly StopPath = '/juqueue/stop';

  /**
   * Stop.
   *
   *
   *
   * This method provides access to the full `HttpResponse`, allowing access to response headers.
   * To access only the response body, use `stop()` instead.
   *
   * This method doesn't expect any request body.
   */
  stop$Response(params?: {
  }): Observable<StrictHttpResponse<any>> {

    const rb = new RequestBuilder(this.rootUrl, JuQueueService.StopPath, 'put');
    if (params) {
    }

    return this.http.request(rb.build({
      responseType: 'json',
      accept: 'application/json'
    })).pipe(
      filter((r: any) => r instanceof HttpResponse),
      map((r: HttpResponse<any>) => {
        return r as StrictHttpResponse<any>;
      })
    );
  }

  /**
   * Stop.
   *
   *
   *
   * This method provides access to only to the response body.
   * To access the full response (for headers, for example), `stop$Response()` instead.
   *
   * This method doesn't expect any request body.
   */
  stop(params?: {
  }): Observable<any> {

    return this.stop$Response(params).pipe(
      map((r: StrictHttpResponse<any>) => r.body as any)
    );
  }

  /**
   * Path part for operation reloadExperiments
   */
  static readonly ReloadExperimentsPath = '/juqueue/reload_experiments';

  /**
   * Reload Experiments.
   *
   *
   *
   * This method provides access to the full `HttpResponse`, allowing access to response headers.
   * To access only the response body, use `reloadExperiments()` instead.
   *
   * This method doesn't expect any request body.
   */
  reloadExperiments$Response(params?: {
  }): Observable<StrictHttpResponse<SuccessResponse>> {

    const rb = new RequestBuilder(this.rootUrl, JuQueueService.ReloadExperimentsPath, 'put');
    if (params) {
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
   * Reload Experiments.
   *
   *
   *
   * This method provides access to only to the response body.
   * To access the full response (for headers, for example), `reloadExperiments$Response()` instead.
   *
   * This method doesn't expect any request body.
   */
  reloadExperiments(params?: {
  }): Observable<SuccessResponse> {

    return this.reloadExperiments$Response(params).pipe(
      map((r: StrictHttpResponse<SuccessResponse>) => r.body as SuccessResponse)
    );
  }

  /**
   * Path part for operation reloadClusters
   */
  static readonly ReloadClustersPath = '/juqueue/reload_clusters';

  /**
   * Reload Clusters.
   *
   *
   *
   * This method provides access to the full `HttpResponse`, allowing access to response headers.
   * To access only the response body, use `reloadClusters()` instead.
   *
   * This method doesn't expect any request body.
   */
  reloadClusters$Response(params?: {
  }): Observable<StrictHttpResponse<SuccessResponse>> {

    const rb = new RequestBuilder(this.rootUrl, JuQueueService.ReloadClustersPath, 'put');
    if (params) {
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
   * Reload Clusters.
   *
   *
   *
   * This method provides access to only to the response body.
   * To access the full response (for headers, for example), `reloadClusters$Response()` instead.
   *
   * This method doesn't expect any request body.
   */
  reloadClusters(params?: {
  }): Observable<SuccessResponse> {

    return this.reloadClusters$Response(params).pipe(
      map((r: StrictHttpResponse<SuccessResponse>) => r.body as SuccessResponse)
    );
  }

}
