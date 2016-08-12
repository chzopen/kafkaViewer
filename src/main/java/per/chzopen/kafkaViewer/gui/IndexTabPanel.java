package per.chzopen.kafkaViewer.gui;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.Box;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;

import per.chzopen.kafkaViewer.gui.KafkaTree.KafkaTreeNode;

public class IndexTabPanel extends JPanel
{

	private static final long serialVersionUID = 1L;
	
	private KafkaTree kafkaTree;
	private JTextArea textarea;
	
	public IndexTabPanel()
	{
		initUI();
		initEvent();
	}
	
	private void initUI()
	{

		this.setLayout(new GridBagLayout());
		
		GridBagConstraints gbc = null;
		int gridy = -1;
		
		// row 0
		{
			gridy++;

			gbc = new GridBagConstraints();
			gbc.gridx = 0;
			gbc.gridy = gridy;
			this.add(Box.createRigidArea(new Dimension(5, 5)), gbc);
		}
		
		// row 1
		{
			gridy++;

			kafkaTree = new KafkaTree();
			kafkaTree.setMessage("Zookeeper not connected");
			
			textarea = new JTextArea();
			
			JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, new JScrollPane(kafkaTree), new JScrollPane(textarea));
			splitPane.setDividerLocation(400);

			gbc = new GridBagConstraints();
			gbc.gridx = 1;
			gbc.gridy = gridy;
			gbc.weightx = 1;
			gbc.weighty = 1;
			gbc.fill = GridBagConstraints.BOTH;
			this.add(splitPane, gbc);
		}

		// row -1
		{
			gridy++;

			gbc = new GridBagConstraints();
			gbc.gridx = 99;
			gbc.gridy = gridy;
			this.add(Box.createRigidArea(new Dimension(5, 5)), gbc);
		}
	
	}
	
	private void initEvent()
	{
		kafkaTree.addListener(new KafkaTree.Listener()
		{
			public void onBrokerData(KafkaTreeNode treeNode, byte[] data) throws Exception
			{
				textarea.setText(new String(data));
				textarea.select(0, 0);
			}
			
			public void onTopicData(KafkaTreeNode treeNode, byte[] data) throws Exception
			{
				textarea.setText(new String(data));
				textarea.select(0, 0);
			}
			
			public void onTopicOpen(KafkaTreeNode treeNode, String topic) throws Exception
			{
			}
			
			public void onPartitionData(KafkaTreeNode treeNode, byte[] data) throws Exception
			{
				textarea.setText(new String(data));
				textarea.select(0, 0);
			}
		});
	}

	public KafkaTree getKafkaTree()
	{
		return kafkaTree;
	}
}
