import type { SVGProps } from "react";

type P = SVGProps<SVGSVGElement>;

const Svg = ({ children, ...p }: P) => (
  <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2"
       strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false" {...p}>
    {children}
  </svg>
);

export const IconUpload = (p: P) => <Svg {...p}><path d="M12 16V4M6 10l6-6 6 6M4 20h16" /></Svg>;
export const IconPause = (p: P) => <Svg {...p}><path d="M9 5v14M15 5v14" /></Svg>;
export const IconPlay = (p: P) => <Svg {...p}><path d="M7 5l12 7-12 7z" /></Svg>;
export const IconX = (p: P) => <Svg {...p}><path d="M6 6l12 12M18 6L6 18" /></Svg>;
export const IconRetry = (p: P) => <Svg {...p}><path d="M4 12a8 8 0 1 0 2.3-5.7M4 4v5h5" /></Svg>;
export const IconTrash = (p: P) => <Svg {...p}><path d="M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3" /></Svg>;
export const IconArrowUp = (p: P) => <Svg {...p}><path d="M12 19V5M6 11l6-6 6 6" /></Svg>;
export const IconEye = (p: P) => <Svg {...p}><path d="M2 12s4-7 10-7 10 7 10 7-4 7-10 7S2 12 2 12z" /><circle cx="12" cy="12" r="3" /></Svg>;
export const IconCheck = (p: P) => <Svg {...p}><path d="M5 12l5 5 9-10" /></Svg>;
export const IconAlert = (p: P) => <Svg {...p}><path d="M12 8v5M12 17h.01M10.3 3.9L2 18a2 2 0 0 0 1.7 3h16.6a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z" /></Svg>;
export const IconOffline = (p: P) => <Svg {...p}><path d="M2 2l20 20M8.5 16.5a5 5 0 0 1 7 0M5 12.9a10 10 0 0 1 5.2-2.8M19 12.9a10 10 0 0 0-2.3-1.6M2 8.8a15 15 0 0 1 4.2-2.7M22 8.8A15 15 0 0 0 11 4.6M12 20h.01" /></Svg>;
