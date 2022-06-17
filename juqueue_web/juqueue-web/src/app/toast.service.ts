import { Injectable, TemplateRef } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class ToastService {

  toasts: any[] = [];

  constructor() { }

  showSuccess(message: string) {
    this.show(message, 
      { classname: 'bg-success text-light', delay: 10000 });
  }
  showError(message: string) {
    this.show(message, 
      { classname: 'bg-danger text-light', delay: 10000 });
  }

  show(textOrTpl: string | TemplateRef<any>, options: any = {}) {
    this.toasts.push({ textOrTpl, ...options });
  }

  remove(toast: any) {
    this.toasts = this.toasts.filter(t => t !== toast);
  }

  clear() {
    this.toasts.splice(0, this.toasts.length);
  }
}
