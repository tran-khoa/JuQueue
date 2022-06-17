import { Injectable } from '@angular/core';
import { isDevMode } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class WebsocketService {

  callbacks: (() => void)[] = [];
  webSocket: WebSocket;

  constructor() {
    if (isDevMode()) {
      this.webSocket = new WebSocket("ws://localhost:8080/ws");
    } else {
      this.webSocket = new WebSocket("ws://" + location.host + "/ws");
    }
    
    this.webSocket.onmessage = (ev: any) => this.notify();
  }

  notify() {
    console.log("notify");
    
    this.callbacks.forEach(fn => fn());
  }

  register_callback(fn: () => void) {
    this.callbacks.push(fn);
  }

}
