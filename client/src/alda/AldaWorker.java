package alda;

public class AldaWorker extends AldaProcess {
  private int workPort;
  private int controlPort;

  public AldaWorker(int workPort, int controlPort)
    throws InvalidOptionsException {
    this.workPort = workPort;
    this.controlPort = controlPort;
  }

  public void upFg()
    throws InvalidOptionsException {
    Object[] args = {this.workPort, this.controlPort};

    Util.callClojureFn("alda.worker/start-worker!", args);
  }
}
