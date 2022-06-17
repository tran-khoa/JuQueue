import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';

import { HttpClientModule } from '@angular/common/http';
import { ApiModule } from './api/api.module';
import { ExperimentListComponent } from './experiment-list/experiment-list.component';
import { NgbModule } from '@ng-bootstrap/ng-bootstrap';
import { NgxBootstrapIconsModule, checkCircleFill, xCircleFill, playCircleFill, pauseCircleFill } from 'ngx-bootstrap-icons';
import { ToastComponent } from './toast/toast.component';
import { FormsModule } from '@angular/forms';
import { ReactiveFormsModule } from '@angular/forms';
import { NgxBootstrapMultiselectModule } from 'ngx-bootstrap-multiselect';
import { ClusterListComponent } from './cluster-list/cluster-list.component';


@NgModule({
  declarations: [
    AppComponent,
    ExperimentListComponent,
    ToastComponent,
    ClusterListComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    HttpClientModule,
    ApiModule,
    NgbModule,
    NgxBootstrapIconsModule.pick({ checkCircleFill, xCircleFill, playCircleFill, pauseCircleFill}),
    FormsModule,
    ReactiveFormsModule,
    NgxBootstrapMultiselectModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
