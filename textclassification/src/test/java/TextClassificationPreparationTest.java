import org.junit.Test;
import utils.TextClassificationPreparation;

import static org.junit.Assert.*;

public class TextClassificationPreparationTest {

    @Test
    public void randomSelectTextsForClassification() {

    }

    @Test
    public void getLastlevelDirFromPath() {
        String [] paths = {"/111/222"};
        for(String path : paths) {
            assertEquals(TextClassificationPreparation.getLastlevelDirFromPath(path), "111");
        }
    }
}