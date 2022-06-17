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

import { ClusterInfo } from '../models/cluster-info';
import { SuccessResponse } from '../models/success-response';

@Injectable({
  providedIn: 'root',
})
export class ClusterService extends BaseService {
  constructor(
    config: ApiConfiguration,
    http: HttpClient
  ) {
    super(config, http);
  }

  /**
   * Path part for operation getClusters
   */
  static readonly GetClustersPath = '/clusters';

  /**
   * Get Clusters.
   *
   *
   *
   * This method provides access to the full `HttpResponse`, allowing access to response headers.
   * To access only the response body, use `getClusters()` instead.
   *
   * This method doesn't expect any request body.
   */
  getClusters$Response(params?: {
  }): Observable<StrictHttpResponse<{
[key: string]: ClusterInfo;
}>> {

    const rb = new RequestBuilder(this.rootUrl, ClusterService.GetClustersPath, 'get');
    if (params) {
    }

    return this.http.request(rb.build({
      responseType: 'json',
      accept: 'application/json'
    })).pipe(
      filter((r: any) => r instanceof HttpResponse),
      map((r: HttpResponse<any>) => {
        return r as StrictHttpResponse<{
        [key: string]: ClusterInfo;
        }>;
      })
    );
  }

  /**
   * Get Clusters.
   *
   *
   *
   * This method provides access to only to the response body.
   * To access the full response (for headers, for example), `getClusters$Response()` instead.
   *
   * This method doesn't expect any request body.
   */
  getClusters(params?: {
  }): Observable<{
[key: string]: ClusterInfo;
}> {

    return this.getClusters$Response(params).pipe(
      map((r: StrictHttpResponse<{
[key: string]: ClusterInfo;
}>) => r.body as {
[key: string]: ClusterInfo;
})
    );
  }

  /**
   * Path part for operation rescaleCluster
   */
  static readonly RescaleClusterPath = '/clusters/{cluster_name}/rescale';

  /**
   * Rescale Cluster.
   *
   *
   *
   * This method provides access to the full `HttpResponse`, allowing access to response headers.
   * To access only the response body, use `rescaleCluster()` instead.
   *
   * This method doesn't expect any request body.
   */
  rescaleCluster$Response(params: {
    cluster_name: string;
  }): Observable<StrictHttpResponse<SuccessResponse>> {

    const rb = new RequestBuilder(this.rootUrl, ClusterService.RescaleClusterPath, 'get');
    if (params) {
      rb.path('cluster_name', params.cluster_name, {});
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
   * Rescale Cluster.
   *
   *
   *
   * This method provides access to only to the response body.
   * To access the full response (for headers, for example), `rescaleCluster$Response()` instead.
   *
   * This method doesn't expect any request body.
   */
  rescaleCluster(params: {
    cluster_name: string;
  }): Observable<SuccessResponse> {

    return this.rescaleCluster$Response(params).pipe(
      map((r: StrictHttpResponse<SuccessResponse>) => r.body as SuccessResponse)
    );
  }

  /**
   * Path part for operation syncCluster
   */
  static readonly SyncClusterPath = '/clusters/{cluster_name}/sync';

  /**
   * Sync Cluster.
   *
   *
   *
   * This method provides access to the full `HttpResponse`, allowing access to response headers.
   * To access only the response body, use `syncCluster()` instead.
   *
   * This method doesn't expect any request body.
   */
  syncCluster$Response(params: {
    cluster_name: string;
  }): Observable<StrictHttpResponse<SuccessResponse>> {

    const rb = new RequestBuilder(this.rootUrl, ClusterService.SyncClusterPath, 'get');
    if (params) {
      rb.path('cluster_name', params.cluster_name, {});
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
   * Sync Cluster.
   *
   *
   *
   * This method provides access to only to the response body.
   * To access the full response (for headers, for example), `syncCluster$Response()` instead.
   *
   * This method doesn't expect any request body.
   */
  syncCluster(params: {
    cluster_name: string;
  }): Observable<SuccessResponse> {

    return this.syncCluster$Response(params).pipe(
      map((r: StrictHttpResponse<SuccessResponse>) => r.body as SuccessResponse)
    );
  }

}
