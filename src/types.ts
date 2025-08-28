export type Job = {
  jid: string,
  errors?: number,
  expiry?: number,
  data: any,
};

export type Logger = {
  error: (...args: any[]) => void;
}