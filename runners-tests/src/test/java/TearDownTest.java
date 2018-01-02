import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;


public class TearDownTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testsLongTearDown() throws Exception {
    TearDown.runPipeline(p);
  }
}
